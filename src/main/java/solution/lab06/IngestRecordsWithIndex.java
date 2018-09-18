package solution.lab06;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.lexicoder.DateLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import solution.BaseClient;
import solution.CrimeFields;

import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IngestRecordsWithIndex extends BaseClient {

    public static void main(String[] args) {
        IngestRecordsWithIndex client = new IngestRecordsWithIndex();
        client.parseArguments(args);
        client.run();
    }

    private static final DateLexicoder DATE_LEXICODER = new DateLexicoder();
    private static final SimpleDateFormat SDF = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
    private static final Text EMPTY_TEXT = new Text();

    public void run() {

        String instanceName = properties.getProperty(INSTANCE);
        String zookeepers = properties.getProperty(ZOOKEEPERS);
        String username = properties.getProperty(USERNAME);
        String password = properties.getProperty(PASSWORD);
        String table = properties.getProperty(TABLE_NAME);
        String columnFamily = properties.getProperty(COLUMN_FAMILY);
        String columnQualifier = properties.getProperty(COLUMN_QAULIFIER);
        String row = properties.getProperty(ROW_ID);


        try {
            System.out.println("Zookeepers: " + zookeepers);
            System.out.println("Connecting to accumulo");
            Instance inst = new ZooKeeperInstance(instanceName, zookeepers);
            Connector conn = inst.getConnector(username, new PasswordToken(password));

            Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
            scanner.fetchColumn(new Text("Attributes"), new Text(CrimeFields.DATE.title()));

            // Configure a Batch writer here
            BatchWriterConfig bwConfig = new BatchWriterConfig();
            bwConfig.setDurability(Durability.DEFAULT);
            bwConfig.setMaxMemory(10240);
            bwConfig.setMaxLatency(1, TimeUnit.SECONDS);
            bwConfig.setMaxWriteThreads(10);

            // Create your batch writer here
            String tableIndexName = table + "_id_index";

            if (!conn.tableOperations().exists(tableIndexName)) {
                System.out.println("Creating table " + tableIndexName);
                conn.tableOperations().create(tableIndexName);
            }

            BatchWriter writer = conn.createBatchWriter(tableIndexName, bwConfig);

            final Value NULL_VALUE = new Value();
            int mutationsWritten = 0;

            // We'll grab the iterator object since we need to advance
            // manually
            for (Map.Entry<Key, Value> entry : scanner) {
                byte[] dateBytes = DATE_LEXICODER.encode(SDF.parse(entry.getValue().toString()));
                String crimeId = entry.getKey().getRow().toString();


                Mutation m = new Mutation(dateBytes);
                m.put(new Text(crimeId), EMPTY_TEXT, NULL_VALUE);
                writer.addMutation(m);

                mutationsWritten++;

                if (mutationsWritten % 1000 == 0) {
                    System.out.println(mutationsWritten + " mutations written");
                }

            }
            // Close resources
            scanner.close();
            writer.close();

            System.out.println("Written a total of " + mutationsWritten);


        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (TableExistsException e) {
            e.printStackTrace();
        }
    }

    private String getPrimaryType(Iterator<Map.Entry<Key, Value>> scannerIterator, Map.Entry<Key, Value> entry) {

        while (!entry.getKey().getColumnQualifier().toString().equals(CrimeFields.PRIMARY_TYPE.title())) {
            if (scannerIterator.hasNext())
                entry = scannerIterator.next();
            else
                return null;
        }

        return entry.getValue().toString();


    }

    private byte[] getDate(Iterator<Map.Entry<Key, Value>> scannerIterator, Map.Entry<Key, Value> entry) throws ParseException {

        while (!entry.getKey().getColumnQualifier().toString().equals(CrimeFields.DATE.title())) {
            if (scannerIterator.hasNext())
                entry = scannerIterator.next();
            else
                return null;
        }

        return DATE_LEXICODER.encode(SDF.parse(new String(entry.getValue().get(), Charset.forName("UTF-8"))));

    }


}

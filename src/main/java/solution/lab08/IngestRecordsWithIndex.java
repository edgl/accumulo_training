package solution.lab08;

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

    // Create a static date lexicoder
    private static final DateLexicoder DATE_LEXICODER = new DateLexicoder();

    // Create a private static field here for a simple Date Format.
    // See if you could determine the format here by going to the shell
    private static final SimpleDateFormat SDF = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");


    private static final Text EMPTY_TEXT = new Text();
    private static final Value NULL_VALUE = new Value();

    public void run() {

        // Grab all the necessary properties here
        String instanceName = properties.getProperty(INSTANCE);
        String zookeepers = properties.getProperty(ZOOKEEPERS);
        String username = properties.getProperty(USERNAME);
        String password = properties.getProperty(PASSWORD);
        String table = properties.getProperty(TABLE_NAME);


        try {
            System.out.println("Zookeepers: " + zookeepers);
            System.out.println("Connecting to accumulo");

            // Normal stuff, create the instance and connector objects
            Instance inst = new ZooKeeperInstance(instanceName, zookeepers);
            Connector conn = inst.getConnector(username, new PasswordToken(password));

            // Create a scanner object, we'll use this to
            // iterate our entire dataset. This is not the most
            // effecient ways to do this. Think of the types of bottlenecks
            // that could occur. What type of optimizations do you think can be done?
            // Would mapreduce be a better option?
            Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);

            // Let's also make sure we fetch the column qualifier for the
            // date field. Use the fetchColumn of the scanner. You could use the
            // CrimeFields.DATE.title() to get the actual text of the CQ that we want
            scanner.fetchColumn(new Text("Attributes"), new Text(CrimeFields.DATE.title()));

            // Configure a Batch writer here
            BatchWriterConfig bwConfig = new BatchWriterConfig();

            // Set the durability, maxmemory, maxlatency and the
            // maxwritethreads of the config. Use the default
            // values we've been doing
            bwConfig.setDurability(Durability.DEFAULT);
            bwConfig.setMaxMemory(10240);
            bwConfig.setMaxLatency(1, TimeUnit.SECONDS);
            bwConfig.setMaxWriteThreads(10);

            // We'll create the index table
            String tableIndexName = table + "_index";

            // Create the table if it doesn't exist
            if (!conn.tableOperations().exists(tableIndexName)) {
                System.out.println("Creating table " + tableIndexName);
                conn.tableOperations().create(tableIndexName);
            }

            // Create a batchwriter object. We'll use this
            // to write to the tables directly? For a big dataset, would this be
            // the most effiecient way? How about writing to a RFile instead?
            // Or using mapreduce? What Input and OutputFormat would you use?
            BatchWriter writer = conn.createBatchWriter(tableIndexName, bwConfig);


            int mutationsWritten = 0;

            for (Map.Entry<Key, Value> entry : scanner) {

                String date = entry.getValue().toString();

                // Since we're limiting our results to only the date CQ
                // Each key value pair would have as a value the date for this crime
                // Use a date lexicoder to encode the date as bytes so we could
                // use this as our Row. Use the date format to parse the date
                // String into a Java date object first.
                byte[] dateBytes = DATE_LEXICODER.encode(SDF.parse(date));

                // Grab the crime id from the key. They key (crimeId) is
                // returned as a Text object and since it's encoded as integers
                // this is safe to convert to a string object
                String crimeId = entry.getKey().getRow().toString();

                // Create the mutation object. What should be the row?
                Mutation m = new Mutation(dateBytes);

                // Put the CF, CQ, and VALUE. The CQ and Value
                // should be empty. Use the private static fields provided
                // so we don't have to keep reusing it.
                m.put(new Text(crimeId), EMPTY_TEXT, NULL_VALUE);

                // Now add the mutation to the writer
                writer.addMutation(m);

                mutationsWritten++;

                if (mutationsWritten % 1000 == 0) {
                    System.out.println(mutationsWritten + " mutations written");
                }

            }
            // Close both the scanner and the writer
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


}

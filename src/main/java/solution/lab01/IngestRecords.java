package solution.lab01;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import solution.BaseClient;
import solution.CrimeFields;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class IngestRecords extends BaseClient {

    private static final Logger LOGGER = Logger.getLogger(IngestRecords.class.getClass());
    private static final SimpleDateFormat SDF = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
    public static void main(String[] args) {
        IngestRecords client = new IngestRecords();
        client.parseArguments(args);
        client.run();
    }


    public void run() {
        String instanceName = properties.getProperty(INSTANCE);
        String zookeepers = properties.getProperty(ZOOKEEPERS);
        String username = properties.getProperty(USERNAME);
        String password = properties.getProperty(PASSWORD);
        String table = properties.getProperty(TABLE_NAME);
        String filename = properties.getProperty(INPUT_FILE);

        int maxLatency = Integer.parseInt(properties.getProperty(MAX_LATENCY, "1"));
        int maxMemory = Integer.parseInt(properties.getProperty(MAX_MEMORY, "10240"));
        int maxThreads = Integer.parseInt(properties.getProperty(MAX_THREADS, "10"));

        try {
            System.out.println("Zookeepers: " + zookeepers);
            System.out.println("Connecting to accumulo");

            // First get the instance (use zookeeper)
            Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
            System.out.println("Instance");

            // Get the connector from the instance
            Connector conn = instance.getConnector(username, new PasswordToken(password));

            System.out.println("Connector");

            // Create the table if it doesn't exist
            if(!conn.tableOperations().exists(table)) {
                System.out.println("Table doesn't exist. Creating table " + table);
                conn.tableOperations().create(table);
            }

            // Set batchwriter configurations
            BatchWriterConfig config = new BatchWriterConfig();
            config.setDurability(Durability.DEFAULT);
            config.setMaxMemory(maxMemory);
            config.setMaxWriteThreads(maxThreads);
            config.setMaxLatency(maxLatency, TimeUnit.SECONDS);

            // Create a batch writer from the connection
            BatchWriter writer = conn.createBatchWriter(table, config);

            System.out.println("writing data from file " + filename + "...");
            final Reader reader = new FileReader(filename);
            final CSVParser csvParser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());

            int recordsWritten = 0;
            int sourceRowsWritten = 0;
            
            for(final CSVRecord record: csvParser) {

                Mutation m = new Mutation(record.get(CrimeFields.ID.title()));
                String primaryType = parseElement(record.get(CrimeFields.PRIMARY_TYPE.title()));

                for (CrimeFields CF: CrimeFields.values()) {
                    Value value;
                    if (CF == CrimeFields.DATE || CF == CrimeFields.UPDATED_ON)
                        value = new Value(record.get(CF.title()).getBytes());
                    else
                        value = new Value(parseElement(record.get(CF.title())).getBytes());

                    m.put("Attributes", CF.title(), value);

                    if (++recordsWritten % 10000 == 0) {
                        System.out.println("Written " + recordsWritten + " mutations so far...");
                    }
                }
                writer.addMutation(m);

                if (++sourceRowsWritten % 10000 == 0) {
                    System.out.println("Written " + sourceRowsWritten + " source line records so far... ");
                }

            }

            // close the Batch Writer
            writer.close();

            // close the Input Reader
            reader.close();

            System.out.println("Written " + recordsWritten + " rows to " + table);

        } catch (AccumuloSecurityException | TableNotFoundException | TableExistsException | AccumuloException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String parseElement(String country) {
        return country.replaceAll("[\"() ]", "").trim();
    }

}

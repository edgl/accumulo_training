package working;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import java.io.*;
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

            // Create an instance
            // CODE
            System.out.println("Instance");

            // Get the connector from the instance
            // CODE
            System.out.println("Connector");

            // Create the table IF it doesn't exist
            // Use table operations
            // CODE

            // Set batchwriter configurations
            BatchWriterConfig config = new BatchWriterConfig();
            config.setDurability(Durability.DEFAULT);
            config.setMaxMemory(maxMemory);
            config.setMaxWriteThreads(maxThreads);
            config.setMaxLatency(maxLatency, TimeUnit.SECONDS);


            // Create a batch writer from the connection

            System.out.println("writing data from file " + filename + "...");
            final Reader reader = new FileReader(filename);
            final CSVParser csvParser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());

            int recordsWritten = 0;
            int sourceRowsWritten = 0;
            for(final CSVRecord record: csvParser) {

                // CREATE A MUTATION and use CrimeFields.ID.title() as the row
                Mutation m = null; // change

                // Get the Primary Type
                String primaryType = parseElement(record.get(CrimeFields.PRIMARY_TYPE.title()));

                // Go through all the fields and add them
                // with "Attribute" colummn family and the field header as the Column
                // qualifier and the value as the cell value
                for (CrimeFields CF: CrimeFields.values()) {
                    Value value = null;


                    if (++recordsWritten % 10000 == 0) {
                        System.out.println("Written " + recordsWritten + " mutations so far...");
                    }
                }

                // Add the mutation to the writer
                // CODE

                if (++sourceRowsWritten % 10000 == 0) {
                    System.out.println("Written " + sourceRowsWritten + " source line records so far... ");
                }

            }

            // close the Batch Writer
            // CODE

            // close the Input Reader
            // CODE

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

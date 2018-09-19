package solution;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GenerateRandom extends BaseClient {

    private static final Random RANDOM = new Random(System.currentTimeMillis());

    public static void main(String[] args) {
        GenerateRandom client = new GenerateRandom();
        client.parseArguments(args);
        client.run();
    }


    public void run() {
        String instanceName = properties.getProperty(INSTANCE);
        String zookeepers = properties.getProperty(ZOOKEEPERS);
        String username = properties.getProperty(USERNAME);
        String password = properties.getProperty(PASSWORD);
        String table = properties.getProperty(TABLE_NAME);


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

            System.out.println("Generating random data 10000");

            Value val = new Value();
            for (int i = 0; i < 10000; i++) {

                Mutation m = new Mutation(Double.toString(RANDOM.nextDouble()));
                m.put("data", Double.toString(RANDOM.nextDouble()), val);
                writer.addMutation(m);
            }

            // close the Batch Writer
            writer.close();


        } catch (AccumuloSecurityException | TableNotFoundException | TableExistsException | AccumuloException e) {
            e.printStackTrace();
        }
    }


}

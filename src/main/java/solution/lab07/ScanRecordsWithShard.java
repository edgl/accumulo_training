package solution.lab07;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import solution.BaseClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ScanRecordsWithShard extends BaseClient {

    private static final IntegerLexicoder LEXICODER = new IntegerLexicoder();

    public static void main(String[] args) {
        ScanRecordsWithShard client = new ScanRecordsWithShard();
        client.parseArguments(args);
        client.run();
    }

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

            // Setup a BatchScanner
            // Batch scanners are more efficient when scanning
            // many rows that aren't contiguous
            BatchScanner batchScanner = conn.createBatchScanner(table, Authorizations.EMPTY, 10);
            batchScanner.setRanges(getListOfRanger(row));


            if (columnFamily != null) {
                if (columnQualifier != null) {
                    batchScanner.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
                }
                else {
                    batchScanner.fetchColumnFamily(new Text(columnFamily));
                }
            }

            for (Map.Entry<Key, Value> entry : batchScanner) {
                System.out.println(
                        entry.getKey().getRow().toString() + " " +
                                entry.getKey().getColumnFamily().toString() + " " +
                                entry.getKey().getColumnQualifier().toString() + "\t" +
                                new String(entry.getValue().get()));
            }

            batchScanner.close();


        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            e.printStackTrace();
        }
    }

    private List<Range> getListOfRanger(String row) {

        List<Range> ranges = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Range range = new Range(new Text("0" + i + "_" + row));
            ranges.add(range);
        }

        return ranges;
    }


}

package solution.lab02;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import solution.BaseClient;

import java.util.Map;

public class ScanRecords extends BaseClient {

    public static void main(String[] args) {
        ScanRecords client = new ScanRecords();
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

            Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);

            if (row != null) {
                scanner.setRange(Range.exact(row));
            }

            if (columnFamily != null) {
                if (columnQualifier != null) {
                    scanner.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
                }
                else {
                    scanner.fetchColumnFamily(new Text(columnFamily));
                }
            }

            for (Map.Entry<Key, Value> entry : scanner) {
                System.out.println(
                        entry.getKey().getRow().toString() + " " +
                                entry.getKey().getColumnFamily().toString() + " " +
                                entry.getKey().getColumnQualifier().toString() + "\t" +
                                new String(entry.getValue().get()));
            }


        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            e.printStackTrace();
        }
    }


}

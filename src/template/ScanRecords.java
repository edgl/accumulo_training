package working;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

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

            // Create your instance and connector
            // Instance inst =
            // Connector conn =

            // Create a scanner using the connector
            // Pass in an empty Authorization for now. Use Authorizations.EMPTY
            // Scanner scanner =

            if (row != null) {
                // Use the scanner object to set the range
                // Use an "exact" range
                // CODE
            }

            if (columnFamily != null) {
                if (columnQualifier != null) {
                    // Set a column family and a column qualifier
                    // using the fetcColumn on the scanner
                    // CODE
                }
                else {
                    // Set just a column family
                    // CODE
                }
            }

            // Scanner implements in iterator. Iterate
            // through all the Map entries and display the results in the
            // console
            // for (... : scanner) {
            //    System.out.println(...);
            //}


        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            e.printStackTrace();
        }
    }


}

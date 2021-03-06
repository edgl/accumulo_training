package solution.lab12;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.lexicoder.DateLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import solution.BaseClient;
import solution.CrimeFields;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.transform;

public class ScanRecordsWithAuths extends BaseClient {

    private static final DateLexicoder DATE_LEXICODER = new DateLexicoder();
    private static final SimpleDateFormat SDF = new SimpleDateFormat("MM/dd/yyyy");

    public static void main(String[] args) {
        ScanRecordsWithAuths client = new ScanRecordsWithAuths();
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
        String startDate = properties.getProperty(START_DATE);
        String endDate = properties.getProperty(END_DATE);
        String primaryType = properties.getProperty(PRIMARY_TYPE);
        String auths = properties.getProperty(AUTHS);

        String tableIndexName = "crimes" + "_index";

        try {
            System.out.println("Zookeepers: " + zookeepers);
            System.out.println("Connecting to accumulo");

            Instance inst = new ZooKeeperInstance(instanceName, zookeepers);
            Connector conn = inst.getConnector(username, new PasswordToken(password));

            // Create an authorizations object
            // If the the properties contains entries
            // for the authorizations, then create
            // authorizations based on those. In our implementation
            // we saying that the auths property is comma delimitted
            Authorizations authz = some default value
            // if auths in the properties file are provided
                // create a new auth object and pass it in
            // CODE
                
            Scanner scanner = conn.createScanner(tableIndexName, authz);

            // Set the range. Remember this is not an 'exact' range
            // so you have to use the constructor that provides a
            // start and end range
            Range ranges = new Range(
                    new Text(DATE_LEXICODER.encode(SDF.parse(startDate))),
                    new Text(DATE_LEXICODER.encode(SDF.parse(endDate)))
            );

            // set the range here
            scanner.setRange(ranges);

            List<Range> idRanges = Lists.newArrayList(
                                            transform(scanner,
                                                new Function<Map.Entry<Key, Value>, Range>() {
                                                    @Override
                                                    public Range apply(Map.Entry<Key, Value> input) {
                                                        Text crimeId = input.getKey().getColumnFamily();
                                                        return Range.exact(crimeId);
                                                    }
                                                }));

            scanner.close();

            System.out.println("Found " + idRanges.size() + " records");

            // Setup a BatchScanner
            // Batch scanners are more efficient when scanning
            // many rows that aren't contiguous
            BatchScanner batchScanner = conn.createBatchScanner(table, authz, 10);
            batchScanner.setRanges(idRanges);

            int count = 0;
            for (Map.Entry<Key, Value> entry : batchScanner) {

                if (entry.getKey().getColumnQualifier().toString().equals(CrimeFields.PRIMARY_TYPE.title()) &&
                    entry.getValue().toString().equals(primaryType)) {
                    count++;
                }
            }

            batchScanner.close();

            System.out.println("Number of " + primaryType + " from " + startDate + " to " + endDate + ": " + count);


        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


}

package solution.lab08;

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

public class ScanRecordsWithIndexClient extends BaseClient {

    // Create a static Date lexicoder object
    private static final DateLexicoder DATE_LEXICODER = new DateLexicoder();

    // Create a static SimpleDateFormat object
    private static final SimpleDateFormat SDF = new SimpleDateFormat("MM/dd/yyyy");

    public static void main(String[] args) {
        ScanRecordsWithIndexClient client = new ScanRecordsWithIndexClient();
        client.parseArguments(args);
        client.run();
    }

    public void run() {

        // Our usual properties
        String instanceName = properties.getProperty(INSTANCE);
        String zookeepers = properties.getProperty(ZOOKEEPERS);
        String username = properties.getProperty(USERNAME);
        String password = properties.getProperty(PASSWORD);
        String table = properties.getProperty(TABLE_NAME);

        // These are the start and end dates that is provided
        // via the properties file
        String startDate = properties.getProperty(START_DATE);
        String endDate = properties.getProperty(END_DATE);

        // Next we specify what type of
        String primaryType = properties.getProperty(PRIMARY_TYPE);

        String tableIndexName = table + "_index";

        try {
            System.out.println("Zookeepers: " + zookeepers);
            System.out.println("Connecting to accumulo");
            Instance inst = new ZooKeeperInstance(instanceName, zookeepers);
            Connector conn = inst.getConnector(username, new PasswordToken(password));

            // First create a regular scanner to scan our index table
            Scanner scanner = conn.createScanner(tableIndexName, Authorizations.EMPTY);

            // Set the range. Remember this is not an 'exact' range
            // so you have to use the constructor that provides a
            // start and end range. Range has a constructor that you
            // could pass in the start and end range as Text objects.
            // Remember, you'll need to encode the dates the have been
            // passed in using the Date lexicoder.
            // DATE_LEXICODER.encode(). You'll also need to convert
            // the date string into a Java Date object first
            Range ranges = new Range(
                    new Text(DATE_LEXICODER.encode(SDF.parse(startDate))),
                    new Text(DATE_LEXICODER.encode(SDF.parse(endDate)))
            );

            // set the range here
            scanner.setRange(ranges);


            // The bottom line here is that you need to
            // go through all the Key-Value pairs, extract
            // the ID from the column qualifier and ceate
            // a list of Ranges which you will then use
            // to create a batch scanner. There are several ways
            // to do this, and it's up to you on how you would like
            // to implement. HINT: might be helpful to use guava's collections
            // to perform some transformations on iterables.
            List<Range> idRanges = Lists.newArrayList(
                                            transform(scanner,
                                                new Function<Map.Entry<Key, Value>, Range>() {
                                                    @Override
                                                    public Range apply(Map.Entry<Key, Value> input) {
                                                        Text crimeId = input.getKey().getColumnFamily();
                                                        return Range.exact(crimeId);
                                                    }
                                                }));

            // Clean up the resources. We don't need
            // the scanner anymore. Better close it
            scanner.close();

            System.out.println("Found " + idRanges.size() + " records in the index table");

            // Setup a BatchScanner
            // Batch scanners are more efficient when scanning
            // many rows that aren't contiguous
            BatchScanner batchScanner = conn.createBatchScanner(table, Authorizations.EMPTY, 10);

            // Set the ranges here
            batchScanner.setRanges(idRanges);

            // limit the range to fetch on the Attributes CF and the
            batchScanner.fetchColumn(new Text("Attributes"), new Text(CrimeFields.PRIMARY_TYPE.title()));

            // Create a counter variable.
            // Remember you'll be getting back the entire
            int count = 0;
            for (Map.Entry<Key, Value> entry : batchScanner) {
                if (entry.getKey().getColumnQualifier().toString().equals(CrimeFields.PRIMARY_TYPE.title()) &&
                        entry.getValue().toString().equals(primaryType)) {
                    count++;
                }
            }

            // Clean up resources
            batchScanner.close();

            // Print out the results
            System.out.println("Number of " + primaryType + " from " + startDate + " to " + endDate + ": " + count);


        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


}

package solution;

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

        String tableIndexName = "crimes_2014" + "_id_index";

        try {
            System.out.println("Zookeepers: " + zookeepers);
            System.out.println("Connecting to accumulo");
            Instance inst = new ZooKeeperInstance(instanceName, zookeepers);
            Connector conn = inst.getConnector(username, new PasswordToken(password));

            Authorizations authz = Authorizations.EMPTY;
            if (auths != null) {
                authz = new Authorizations(auths.split(","));
            }

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

            System.out.println(primaryType + ": " + count);


        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


}

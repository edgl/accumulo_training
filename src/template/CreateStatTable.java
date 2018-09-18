package working;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.lexicoder.DateLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CreateStatTable extends BaseClient {

    public static void main(String[] args) {
        CreateStatTable client = new CreateStatTable();
        client.parseArguments(args);
        client.run();
    }

    private static final DateLexicoder DATE_LEXICODER = new DateLexicoder();
    private static final SimpleDateFormat SDF = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
    private static final Text EMPTY_TEXT = new Text();
    private static final Value ONE_VALUE = new Value("1".getBytes());

    public void run() {

        String instanceName = properties.getProperty(INSTANCE);
        String zookeepers = properties.getProperty(ZOOKEEPERS);
        String username = properties.getProperty(USERNAME);
        String password = properties.getProperty(PASSWORD);
        String table = properties.getProperty(TABLE_NAME);
        String filename = properties.getProperty(INPUT_FILE);


        try {
            System.out.println("Zookeepers: " + zookeepers);
            System.out.println("Connecting to accumulo");

            // Create your instance and connector object
            // Instance inst =
            // Connector conn =

            // Configure a Batch writer here
            BatchWriterConfig bwConfig = new BatchWriterConfig();
            bwConfig.setDurability(Durability.DEFAULT);
            bwConfig.setMaxMemory(10240);
            bwConfig.setMaxLatency(1, TimeUnit.SECONDS);
            bwConfig.setMaxWriteThreads(10);

            // Create a multitable bacth writer from the connection object
            // CODE

            String tableIndexName = table + "_index";

            if (!conn.tableOperations().exists(table)) {
                System.out.println("Creating table " + table);
                conn.tableOperations().create(table);
            }

            if (!conn.tableOperations().exists(tableIndexName)) {
                System.out.println("Creating table " + tableIndexName);
                conn.tableOperations().create(tableIndexName);

                // Use the table operations to remove
                // the versioning interators for all scopes
                // CODE

                // setup combining iterator
                // CODE
                // 1. Create IteratorSetting object
                // 2.
                // 2. Assign BigDecimalCombiner.BigDecimalSummingCombiner.class
                // 3. Use static functions of the BigDecimalSummingCombiner to set
                //    properties such as the columns (use the "sum" column) and
                //    set the combin all columns to 'false'


                // Attach the iterators to the table using the
                // TableOperations interface available to the connector


            }

            // Get a batch writer for the MAIN table
            // and use the Multitable Batch writer
            // BatchWriter mainTableWriter =


            // Get a batch writer for the MAIN table
            // and use the Multitable Batch writer
            // BatchWriter indexTableWriter =


            int mutationsWritten = 0;

            final Text sumText = new Text("sum");
            final Text avgText = new Text("avg");

            System.out.println("writing data from file " + filename + "...");
            final Reader reader = new FileReader(filename);
            final CSVParser csvParser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());

            int recordsWritten = 0;
            int sourceRowsWritten = 0;

            for(final CSVRecord record: csvParser) {

                // Add mutation for the main table
                final Mutation m = new Mutation(record.get(CrimeFields.ID.title()));
                final Text primaryType = new Text(parseElement(record.get(CrimeFields.PRIMARY_TYPE.title())));

                for (CrimeFields CF: CrimeFields.values()) {

                    Value value;
                    if (CF == CrimeFields.DATE || CF == CrimeFields.UPDATED_ON)
                        value = new Value(record.get(CF.title()).getBytes());
                    else
                        value = new Value(parseElement(record.get(CF.title())).getBytes());

                    m.put("Attributes", CF.title(), value);

                    if (++recordsWritten % 10000 == 0)
                        System.out.println("Written " + recordsWritten + " mutations so far...");
                }

                // Add the mutation to the MAIN table batchwriter

                // Add some information output statements to console
                if (++sourceRowsWritten % 10000 == 0) {
                    System.out.println("Written " + sourceRowsWritten + " source line records so far... ");
                }

                // Ingest mutation for the index table
                Date date =  SDF.parse(record.get(CrimeFields.DATE.title()));
                String yearMonthDay = String.format("%04d%02d%02d", date.getYear() + 1900, date.getMonth() + 1, date.getDate());

                // Create mutations with the following rows:
                // 1. YEAR
                // 2. YEARMONTH
                // 3. YEARMONTHDAY
                // For column family, put the "sum" and for the
                // value, put in a Value of "1"

                // Mutation 1
                // addMutation to INDEX table batchwriter

                // Mutation 2
                // addMutation to INDEX table batchwriter

                // Mutation 3
                // addMutation to INDEX table batchwriter

                mutationsWritten++;

                if (mutationsWritten % 1000 == 0) {
                    System.out.println(mutationsWritten + " Main Table mutations written");
                }
            }

            // Close resources, you only need to close the resources for the
            // multitable batch writer object

            System.out.println("Written a total of " + mutationsWritten + " written to main table");

        } catch (IOException | TableExistsException | AccumuloSecurityException | TableNotFoundException | AccumuloException | ParseException e) {
            e.printStackTrace();
        }
    }

    private static String parseElement(String country) {
        return country.replaceAll("[\"() ]", "").trim();
    }


}

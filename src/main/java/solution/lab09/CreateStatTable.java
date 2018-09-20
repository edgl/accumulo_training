package solution.lab09;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.lexicoder.DateLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;
import solution.BaseClient;
import solution.CrimeFields;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

public class CreateStatTable extends BaseClient {

    public static void main(String[] args) {
        CreateStatTable client = new CreateStatTable();
        client.parseArguments(args);
        client.run();
    }

    private static final SimpleDateFormat SDF = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
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

            Instance inst = new ZooKeeperInstance(instanceName, zookeepers);
            Connector conn = inst.getConnector(username, new PasswordToken(password));

            // Configure a Batch writer here
            BatchWriterConfig bwConfig = new BatchWriterConfig();
            bwConfig.setDurability(Durability.DEFAULT);
            bwConfig.setMaxMemory(10240);
            bwConfig.setMaxLatency(1, TimeUnit.SECONDS);
            bwConfig.setMaxWriteThreads(10);

            // Create the multitable batch writer
            // Multitable batch writers allow more effecient way
            // of writing to multiple tables.
            MultiTableBatchWriter multiTableBatchWriter = conn.createMultiTableBatchWriter(bwConfig);

            // Create your batch writer here
            String tableStatsName = table + "_stats";

            if (!conn.tableOperations().exists(tableStatsName)) {
                System.out.println("Creating table " + tableStatsName);
                conn.tableOperations().create(tableStatsName);

                // First we need to remove the versioning iterator.
                // The iterator name is called "vers". For the
                // scope, use EnumSet.allOf(IteratorUtil.IteratorScope.class). Refer to the
                // lab notes or the  solution as well.
                conn.tableOperations().removeIterator(tableStatsName, "vers", EnumSet.allOf(IteratorUtil.IteratorScope.class));

                // Time to setup the iterators
                // First create the IteratorSetting object
                IteratorSetting iterSetting = new IteratorSetting(19, "sum", BigDecimalCombiner.BigDecimalSummingCombiner.class);

                // Next we'll apply this to the column called "sum". So anything we put
                // in this column, the combiner will combine the values and sum them up
                IteratorSetting.Column columnSum = new IteratorSetting.Column("sum");
                BigDecimalCombiner.setColumns(iterSetting, Arrays.asList(columnSum));

                // We want sums for individual types of crimes. So set the combine All columns
                // to false
                BigDecimalCombiner.BigDecimalSummingCombiner.setCombineAllColumns(iterSetting, false);

                // use the table operations to attach the iterator
                // to the stats table
                conn.tableOperations().attachIterator(tableStatsName, iterSetting);

            }

            // Create two batch writers NOT from the connection
            // object, but from the multi table batch writer you
            // setup earlier. use the "getBatchWriter(TABLENAME)"
            BatchWriter mainTableWriter = multiTableBatchWriter.getBatchWriter(table);
            BatchWriter indexTableWriter = multiTableBatchWriter.getBatchWriter(tableStatsName);


            int mutationsWritten = 0;

            // This will be used for the CF name for the stats table
            final Text sumText = new Text("sum");

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

                // Add the mutation to the main table!
                mainTableWriter.addMutation(m);

                // Add some information output statements to console
                if (++sourceRowsWritten % 10000 == 0) {
                    System.out.println("Written " + sourceRowsWritten + " source line records so far... ");
                }

                // Ingest mutation for the index table
                // In addition, and at the same time, we'll
                // go ahead and write to the stats table. First
                // we'll want to extract the DATE field
                Date date =  SDF.parse(record.get(CrimeFields.DATE.title()));
                String yearMonthDay = String.format("%04d%02d%02d", date.getYear() + 1900, date.getMonth() + 1, date.getDate());

                // We're going to want to be able to sum per day, month
                // and year. So we are going to have 3 mutations that
                // we'll create. Firs the year. Create the mutation,
                // put CF and CQ and for VALUE use the ONE_VALUE static field.
                // Firs the YEAR
                Mutation mIdx = new Mutation(yearMonthDay.substring(0, 4));
                mIdx.put(sumText, primaryType, ONE_VALUE);
                indexTableWriter.addMutation(mIdx);

                // Now, do the YEARMON
                mIdx = new Mutation(yearMonthDay.substring(0, 6));
                mIdx.put(sumText, primaryType, ONE_VALUE);
                indexTableWriter.addMutation(mIdx);

                // Now, do the YEARMONDAY
                mIdx = new Mutation(yearMonthDay.substring(0, 8));
                mIdx.put(sumText, primaryType, ONE_VALUE);
                indexTableWriter.addMutation(mIdx);

                // This tracks mutation written to the main table.
                // Feel free to add print statements to keep track
                // of how many mutation written to the stats.
                mutationsWritten++;

                if (mutationsWritten % 1000 == 0) {
                    System.out.println(mutationsWritten + " Main Table mutations written");
                }
            }

            // Close resources
            // You don't need to close indivisual batch writers. Closing
            // the multitable bactchwriter causes all writers to be closed
            multiTableBatchWriter.close();

            System.out.println("Written a total of " + mutationsWritten + " written to main table");

        } catch (IOException | TableExistsException | AccumuloSecurityException | TableNotFoundException | AccumuloException | ParseException e) {
            e.printStackTrace();
        }
    }

    private static String parseElement(String country) {
        return country.replaceAll("[\"() ]", "").trim();
    }


}

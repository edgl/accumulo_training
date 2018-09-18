package solution;

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
            Instance inst = new ZooKeeperInstance(instanceName, zookeepers);
            Connector conn = inst.getConnector(username, new PasswordToken(password));

            // Configure a Batch writer here
            BatchWriterConfig bwConfig = new BatchWriterConfig();
            bwConfig.setDurability(Durability.DEFAULT);
            bwConfig.setMaxMemory(10240);
            bwConfig.setMaxLatency(1, TimeUnit.SECONDS);
            bwConfig.setMaxWriteThreads(10);

            MultiTableBatchWriter multiTableBatchWriter = conn.createMultiTableBatchWriter(bwConfig);

            // Create your batch writer here
            String tableIndexName = table + "_index";

            if (!conn.tableOperations().exists(table)) {
                System.out.println("Creating table " + table);
                conn.tableOperations().create(table);
            }

            if (!conn.tableOperations().exists(tableIndexName)) {
                System.out.println("Creating table " + tableIndexName);
                conn.tableOperations().create(tableIndexName);

                // remove vers iterator
                conn.tableOperations().removeIterator(tableIndexName, "vers", EnumSet.allOf(IteratorUtil.IteratorScope.class));

                // setup combining iterator
                IteratorSetting iterSetting = new IteratorSetting(19, "sum", BigDecimalCombiner.BigDecimalSummingCombiner.class);
                IteratorSetting.Column columnSum = new IteratorSetting.Column("sum");
                BigDecimalCombiner.setColumns(iterSetting, Arrays.asList(columnSum));
                BigDecimalCombiner.BigDecimalSummingCombiner.setCombineAllColumns(iterSetting, false);

                conn.tableOperations().attachIterator(tableIndexName, iterSetting);

            }

            BatchWriter mainTableWriter = multiTableBatchWriter.getBatchWriter(table);
            BatchWriter indexTableWriter = multiTableBatchWriter.getBatchWriter(tableIndexName);


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

                // Add the mutation
                //mainTableWriter.addMutation(m);

                // Add some information output statements to console
                if (++sourceRowsWritten % 10000 == 0) {
                    System.out.println("Written " + sourceRowsWritten + " source line records so far... ");
                }

                // Ingest mutation for the index table
                Date date =  SDF.parse(record.get(CrimeFields.DATE.title()));
                String yearMonthDay = String.format("%04d%02d%02d", date.getYear() + 1900, date.getMonth() + 1, date.getDate());

                Mutation mIdx = new Mutation(yearMonthDay.substring(0, 4));
                mIdx.put(sumText, primaryType, ONE_VALUE);
                indexTableWriter.addMutation(mIdx);

                mIdx = new Mutation(yearMonthDay.substring(0, 6));
                mIdx.put(sumText, primaryType, ONE_VALUE);
                indexTableWriter.addMutation(mIdx);

                mIdx = new Mutation(yearMonthDay.substring(0, 8));
                mIdx.put(sumText, primaryType, ONE_VALUE);
                indexTableWriter.addMutation(mIdx);

                mutationsWritten++;

                if (mutationsWritten % 1000 == 0) {
                    System.out.println(mutationsWritten + " Main Table mutations written");
                }
            }

            // Close resources
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

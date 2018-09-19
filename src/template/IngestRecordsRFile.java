package solution.lab03;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import solution.BaseClient;
import solution.CrimeFields;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class IngestRecordsRFile extends BaseClient {

    private static final Logger LOGGER = Logger.getLogger(IngestRecordsRFile.class.getClass());
    private static final SimpleDateFormat SDF = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a");
    
    
    public static void main(String[] args) {
        IngestRecordsRFile client = new IngestRecordsRFile();
        client.parseArguments(args);
        client.run();
    }


    public void run() {

        try {
            String filename = properties.getProperty(INPUT_FILE);
            String outputFile = properties.getProperty(OUTPUT_FILE);

            // create a hadoop configuration object
            // CODE

            // create a filesystem obbject using the configuration object
            // created above
            // CODE

            System.out.println("writing data from file " + filename + "...");
            final Reader reader = new FileReader(filename);
            final CSVParser csvParser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());

            // Record some metrics
            int recordsWritten = 0;
            int sourceRowsWritten = 0;
            int bufferCounter = 0;
            int fileCounter = 0;

            // create a new RFileOperations object
            // CODE

            String hdfsOutputFile = outputFile + "_" + fileCounter + "." + RFile.EXTENSION;

            /*
             * create the FileSKWriter using the RFileOperations.openWriter
             * object. According to the docs, it requires
             * a few parameters:
             *     file - this is the HDFS file it will write to
             *     FileSystem object
             *     Hadoop configuration object
             *     AccumuloConfiguration - use the AccumuloConfiguration.getDefaultConfiguration
             *
             */
            // CODE

            // set the default locality group
            // CODE


            // uses a sorted map, call it buffer
            // CODE

            for(final CSVRecord record: csvParser) {

                if (++bufferCounter % 100000 == 0) {
                    System.out.println("Writing buffer to rfile");
                    for (Map.Entry<Key, Value> entry : buffer.entrySet()) {
                        writer.append(entry.getKey(), entry.getValue());
                    }

                    // clear the buffer
                    buffer.clear();

                    // Close the writer object to create the HDFS file
                    // CODE

                    fileCounter++;

                    // we're creating a new file, so increment the filename
                    hdfsOutputFile = outputFile + "_" + fileCounter + "." + RFile.EXTENSION;

                    // Create a new FileSKWriter object.
                    // You could reuse the same variable as above
                    // CODE

                    // once again start a new default locality group
                    // CODE

                }

                String primaryType = parseElement(record.get(CrimeFields.PRIMARY_TYPE.title()));
                Text row = new Text(record.get(CrimeFields.ID.title()));

                for (CrimeFields CF: CrimeFields.values()) {
                    Key metaKey = new Key(row, new Text("Attributes"), new Text(CF.title()));
                    Value value = new Value(parseElement(record.get(CF.title())).getBytes());
                    buffer.put(metaKey, value);

                    if (++recordsWritten % 10000 == 0) {
                        System.out.println("Written " + recordsWritten + " key-values so far...");
                    }
                }

                if (++sourceRowsWritten % 10000 == 0) {
                    System.out.println("Written " + sourceRowsWritten + " source line records so far... ");
                }

            }

            // Flush if not empty
            if (!buffer.isEmpty()) {
                System.out.println("Writing buffer to rfile");
                for (Map.Entry<Key, Value> entry : buffer.entrySet()) {
                    writer.append(entry.getKey(), entry.getValue());
                }

                writer.close();
                buffer.clear();
            }

            // close the rfile writer
            // CODE

            // close the Input Reader
            reader.close();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String parseElement(String country) {
        return country.replaceAll("[\"() ]", "").trim();
    }

}

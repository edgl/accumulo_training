package working;

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

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);


            System.out.println("writing data from file " + filename + "...");
            final Reader reader = new FileReader(filename);
            final CSVParser csvParser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());

            int recordsWritten = 0;
            int sourceRowsWritten = 0;

            // RFiles need to be sorted. Since we're not letting
            // the tablet servers do this for us, we'll have to
            // do it ourselves
            SortedMap<Key, Value> buffer = new TreeMap<>();

            int bufferCounter = 0;
            int fileCounter = 0;


            // Create the FileSKVWriter object using the RFileOperations object.
            String outFile = properties.getProperty(OUTPUT_FILE) + "_" + fileCounter + "." + RFile.EXTENSION;
            FileSKVWriter writer = 
            
            writer.startDefaultLocalityGroup();
            for(final CSVRecord record: csvParser) {

                if (++bufferCounter % 100000 == 0) {
                    System.out.println("Writing buffer to rfile");
                    for (Map.Entry<Key, Value> entry : buffer.entrySet()) {
                        writer.append(entry.getKey(), entry.getValue());
                    }

                    // close the writer
                    //CODE 
                    
                    fileCounter++;
                    
                    // Reset the writer (create another FileSKVWriter
                    outFile = 
                    writer = 
                    writer.startDefaultLocalityGroup();
                    
                    // clear the buffer
                    buffer.clear();
                }


                String primaryType = parseElement(record.get(CrimeFields.PRIMARY_TYPE.title()));
                Text row = new Text(record.get(CrimeFields.ID.title()));

                for (CrimeFields CF: CrimeFields.values()) {
                	// Create the key and value object and put it in the buffer
                    Key metaKey = 
                    Value value = 
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

            // close the Batch Writer
            writer.close();

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

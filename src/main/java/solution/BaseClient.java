package solution;

import org.apache.commons.cli.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BaseClient {

    private static final Logger LOGGER = Logger.getLogger(BaseClient.class.getName());

    protected static final String INSTANCE = "instance";
    protected static final String ZOOKEEPERS = "zookeepers";
    protected static final String USERNAME = "username";
    protected static final String PASSWORD = "password";
    protected static final String TABLE_NAME = "tableName";
    protected static final String MAX_LATENCY = "maxLatency";
    protected static final String MAX_MEMORY = "maxMemory";
    protected static final String MAX_THREADS = "maxThreads";
    protected static final String INPUT_FILE = "inputFile";
    protected static final String COLUMN_FAMILY = "columnFamily";
    protected static final String COLUMN_QAULIFIER = "columnQualifier";
    protected static final String ROW_ID = "rowId";
    protected static final String OUTPUT_FILE = "outputFile";
    protected static final String START_DATE = "startDate";
    protected static final String END_DATE = "endDate";
    protected static final String PRIMARY_TYPE = "primaryType";
    protected static final String AUTHS = "auths";

    protected Options options = new Options();
    protected CommandLine commandLine;
    protected static Properties properties;

    public void parseArguments(String[] args) {
        System.out.println("Parsing Options");
        parseOptions();
        System.out.println("Parsing Commandline args");
        parseCommandLineArguments(args);
        System.out.println("Loading properties");
        loadProperties();
    }

    private void parseCommandLineArguments(String[] args) {
        CommandLineParser parser = new BasicParser();
        try {
            commandLine = parser.parse(options, args, false);
            System.out.println(commandLine);
            if (commandLine.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("Help", options);
                System.exit(0);
            }
        } catch (ParseException e) {
            System.out.println("Error " + e.toString());
            LOGGER.log(Level.ALL, e.toString());
        }

    }

    public void parseOptions() {
        Option libjars = OptionBuilder.withArgName("libjars")
                .hasArg()
                .withLongOpt("libjars")
                .withDescription("libjars")
                .create("libjars");
        options.addOption(libjars);


        Option type = OptionBuilder.withArgName("p")
                .hasArg()
                .withLongOpt("properties")
                .withDescription("Location of properties file")
                .create("p");
        options.addOption(type);

        Option help = OptionBuilder.withArgName("h")
                .hasArg(false)
                .withLongOpt("help")
                .withDescription("Help")
                .create("h");

        options.addOption(help);
    }

    public Properties loadProperties() {
        if (this.properties == null) {
            properties = new Properties();
            System.out.println(commandLine);
            String propertiesFile = commandLine.getOptionValue("p");
            try {
                properties.load(new FileReader(propertiesFile));
            } catch (IOException e) {
                LOGGER.log(Level.ALL, e.toString());
                throw new RuntimeException(e);
            }
        }

        return properties;
    }

    public abstract void run();

}

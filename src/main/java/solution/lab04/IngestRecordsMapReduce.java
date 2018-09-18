package solution.lab04;

import java.io.IOException;

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import solution.CrimeFields;

public class IngestRecordsMapReduce implements Tool {

   
    private Configuration conf = null;
    
   
    private static String parseElement(String country) {
        return country.replaceAll("[\"() ]", "").trim();
    }


	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}


	@Override
	public Configuration getConf() {
		return this.conf;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new IngestRecordsMapReduce(), args);
		
	}


	@Override
	public int run(String[] args) throws Exception {
		String instanceName = "hdp-accumulo-instance";
        String zookeepers = "localhost:2181";
        String username = "root";
        String password = "bigdata";
        String tableName = "crimes_bulk_mr";
        String inputPath = "/data/csv";
        String outputDirectory = "/tmp/crimes_mr/";
        String failedDirectory = "/tmp/crimes_fail";

        
        Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
        Connector connector = instance.getConnector(username, new PasswordToken(password));
        
        if(!connector.tableOperations().exists(tableName)) {
        	connector.tableOperations().create(tableName);
        }
        
        Job job = Job.getInstance(this.getConf());
        job.setJobName("bulk ingest example");
        job.setJarByClass(this.getClass());

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);

        job.setReducerClass(ReducerClass.class);
        job.setOutputFormatClass(AccumuloFileOutputFormat.class);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setProperty(ClientProperty.INSTANCE_NAME, instanceName);
        clientConfiguration.setProperty(ClientProperty.INSTANCE_ZK_HOST, zookeepers);


        TextInputFormat.setInputPaths(job, inputPath);
        AccumuloFileOutputFormat.setOutputPath(job, new Path(outputDirectory));

        // usually calculated based on the number of splits
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
        
        FileSystem fs = FileSystem.get(conf);
        Path failures = new Path(failedDirectory);
        fs.delete(failures, true);
        fs.mkdirs(new Path(failedDirectory));

        
        // With HDFS permissions on, we need to make sure the Accumulo user can read/move the rfiles
        FsShell fsShell = new FsShell(conf);
        fsShell.run(new String[] {"-chmod", "-R", "777", outputDirectory});
        fsShell.run(new String[] {"-chmod", "-R", "777", failedDirectory});
        
        // Bulk Import it!
        connector.tableOperations().importDirectory(tableName, outputDirectory, failedDirectory, false);
	    
        return 0;
	}
	
	public static class MapClass extends Mapper<LongWritable, Text, Key, Value> {
//		private Text outputKey = new Text();
//		private Text outputValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Key, Value>.Context context)
				throws IOException, InterruptedException {
			
			if(value.toString().startsWith("ID"))
				return;
			
		
			String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			Text row = new Text(tokens[0]);
			
			for (CrimeFields CF: CrimeFields.values()) {
                Key keyOut = new Key(row, new Text("Attributes"), new Text(CF.title()));
                Value valueOut = new Value(parseElement(tokens[CF.ordinal()]).getBytes());
                context.write(keyOut, valueOut);
            }
			
		}
		
	}
	
	public static class ReducerClass extends Reducer<Key, Value, Key, Value> {
		
		@Override
		protected void reduce(Key keyIn, Iterable<Value> valueIn, Reducer<Key, Value, Key, Value>.Context context)
				throws IOException, InterruptedException {
			
			for(Value val: valueIn) {
				context.write(keyIn, val);
			}
		}
		
		
	}


}

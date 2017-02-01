package custompartitioner;
 
import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.conf.Configured;
 import org.apache.hadoop.fs.Path;
 //import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 import org.apache.hadoop.util.Tool;
 import org.apache.hadoop.util.ToolRunner;
 
 
public class PartitionerDriver extends Configured implements Tool{
 
@Override
 public int run(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = new Job(conf, "partitioner");
 
job.setJarByClass(getClass());
 
// configure output and input source
 TextInputFormat.addInputPath(job, new Path(args[0]));
 job.setInputFormatClass(TextInputFormat.class);
 
job.setMapperClass(PartitionerMapper.class);
 job.setPartitionerClass(AgePartitioner.class);
 job.setReducerClass(PartitionerReducer.class);
 
// the number of reducers is set to 3, this can be altered according to
 // the program's requirements
 job.setNumReduceTasks(3);
 
// configure output
 TextOutputFormat.setOutputPath(job, new Path(args[1]));
 job.setOutputFormatClass(TextOutputFormat.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
 
return job.waitForCompletion(true) ? 0 : 1;
 }
 public static void main(String[] args) throws Exception {
 int exitCode = ToolRunner.run(new PartitionerDriver(), args);
 System.exit(exitCode);
 }
 }
package secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        if(args.length < 2){
            System.out.println("Please supply in input and output path");
            System.exit(1);
        }
        
        int exitCode = ToolRunner.run(new SecondarySortDriver(), args);
        System.exit(exitCode);

        

    }

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(new Configuration());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJarByClass(SecondarySortDriver.class);
        job.setOutputKeyClass(TemperaturePair.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(SecondarySortingTemperatureMapper.class);
        job.setPartitionerClass(TemperaturePartitioner.class);
        job.setGroupingComparatorClass(YearMonthGroupingComparator.class);
        job.setReducerClass(SecondarySortingTemperatureReducer.class);
       return job.waitForCompletion(true) ? 0 : 1;
	}
}

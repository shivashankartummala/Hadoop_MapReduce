package nyse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NYSEYearlyAnalysis extends Configured implements Tool {

 /*
  * This mapper processes the NYSE Prices file. Refer to Point 2 in Design
  * Considerations
  */

 public static class PricesMapper extends Mapper<LongWritable, Text, NYSESymbolYearWritable, NYSEWritable> {
  NYSESymbolYearWritable mapOutKey = new NYSESymbolYearWritable();
  NYSEWritable mapOutValue = new NYSEWritable();

  @SuppressWarnings("deprecation")
  @Override
  public void map(
    LongWritable mapInKey,
    Text mapInValue,
    Context context) throws IOException, InterruptedException {
   String[] mapInFieldsArray = mapInValue.toString().split(",");
   DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
   if (mapInFieldsArray.length == 9
     & mapInFieldsArray[0].trim().equals("exchange") == false) {
    /* Set the Map Output Key Variables */
    mapOutKey.setStock_symbol(mapInFieldsArray[1]);
    try {
     /*
      * Note that the getYear method returns the year subtracting
      * 1900 from the year value. And hence we need to add 1900
      * to output year
      */
     mapOutKey.setStock_year(df.parse(mapInFieldsArray[2])
       .getYear() + 1900);
    } catch (ParseException e) {
     System.out.println("Not a valid date : "
       + mapInFieldsArray[2]);
     e.printStackTrace();
    }
    /* Set the Map Output Values Variables */
    mapOutValue.setFile_identifier("prices");
    mapOutValue.setStock_date(mapInFieldsArray[2]);
    mapOutValue.setStock_dividend(0.0);
    mapOutValue.setStock_exchange(mapInFieldsArray[0]);
    mapOutValue.setStock_price_adj_close(Double
      .valueOf(mapInFieldsArray[8]));
    mapOutValue.setStock_price_close(Double
      .valueOf(mapInFieldsArray[6]));
    mapOutValue.setStock_price_high(Double
      .valueOf(mapInFieldsArray[4]));
    mapOutValue.setStock_price_low(Double
      .valueOf(mapInFieldsArray[5]));
    mapOutValue.setStock_price_open(Double
      .valueOf(mapInFieldsArray[3]));
    mapOutValue.setStock_symbol(mapInFieldsArray[1]);
    mapOutValue.setStock_volume(Integer
      .valueOf(mapInFieldsArray[7]).longValue());
    context.write(mapOutKey, mapOutValue);
   }
  }
 }

 /*
  * This mapper processes the NYSE Dividend file. Refer to Point 2 in Design
  * Considerations
  */

 public static class DividendMapper extends Mapper<LongWritable, Text, NYSESymbolYearWritable, NYSEWritable> {
  NYSESymbolYearWritable mapOutKey = new NYSESymbolYearWritable();
  NYSEWritable mapOutValue = new NYSEWritable();

  @SuppressWarnings("deprecation")
  @Override
  public void map(
    LongWritable mapInKey,
    Text mapInValue,
    Context context) throws IOException, InterruptedException {
   String[] mapInFieldsArray = mapInValue.toString().split(",");
   DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
   if (mapInFieldsArray.length == 4
     & mapInFieldsArray[0].trim().equals("exchange") == false) {
    /* Set the Map Output Key Variables */
    mapOutKey.setStock_symbol(mapInFieldsArray[1]);
    try {
     mapOutKey.setStock_year(df.parse(mapInFieldsArray[2])
       .getYear() + 1900);
    } catch (ParseException e) {
     System.out.println("Not a valid date : "
       + mapInFieldsArray[2]);
     e.printStackTrace();
    }
    /* Set the Map Output Values Variables */
    mapOutValue.setFile_identifier("dividends");
    mapOutValue.setStock_date(mapInFieldsArray[2]);
    mapOutValue.setStock_dividend(Double
      .valueOf(mapInFieldsArray[3]));
    mapOutValue.setStock_exchange(mapInFieldsArray[0]);
    mapOutValue.setStock_price_adj_close(0.0);
    mapOutValue.setStock_price_close(0.0);
    mapOutValue.setStock_price_high(0.0);
    mapOutValue.setStock_price_low(0.0);
    mapOutValue.setStock_price_open(0.0);
    mapOutValue.setStock_symbol(mapInFieldsArray[1]);
    mapOutValue.setStock_volume(0);
    context.write(mapOutKey, mapOutValue);
   }
  }
 }

 public static class MyReducer extends Reducer<NYSESymbolYearWritable, NYSEWritable, Text, Text> {
  Text reduceOutKey = new Text();
  Text reduceOutValue = new Text();
  Map<String, String> companiesMap = new HashMap<String, String>();
  private Configuration conf;
  private MultipleOutputs multipleOutputs;
  private String delimiter = "|";

  /*
   * Read the companies file and load it to a HashMap to be used as
   * Distributed Cache. The HashMap will have the Symbol as the Key and
   * Full company name as Value. See Point 4 in Design Considerations.
   */
  public void loadMap(Context context) throws IOException, InterruptedException {
	  Configuration configuration = context.getConfiguration();
	  String cacheFile = configuration.get("job.distcache.cachefile");
		cacheFile = cacheFile.substring(cacheFile.lastIndexOf("/") + 1);
		delimiter = configuration.get("job.distcache.delimiter");

		// Exit if the cache is empty
		if (context.getCacheFiles() == null
				|| context.getCacheFiles().length == 0) {
			System.err.println("Cannot find the cache file");
			System.exit(1);
		}

   
   
   BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(cacheFile)));
   String line = "";
   while ((line = bufferedReader.readLine()) != null) {
    String[] companiesArr = line.split("\t");
    if (companiesArr.length == 2) {
     companiesMap.put(companiesArr[0].trim(), companiesArr[1]);
    }
   }
   bufferedReader.close();
   /*
    * for (Map.Entry<String, String> mE : companiesMap .entrySet()) {
    * System.out.println("atom : key - " + mE.getKey() + " value - " +
    * mE.getValue()); }
    */
  }

 
  public void setup(Context context) throws IOException,InterruptedException {
	  
	 
	  multipleOutputs = new MultipleOutputs<Text, Text>(context);
   try {
    loadMap(context);
   } catch (IOException | InterruptedException e) {
    System.out.println(" Error calling loadMap");
    e.printStackTrace();
   }
  }
  
  @Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs.close();
	}


  @Override
  public void reduce(NYSESymbolYearWritable reduceInKey,java.lang.Iterable<NYSEWritable> reduceInValues, Context context)
    throws IOException, InterruptedException {

   double maxStockPrice = 0;
   double minStockPrice = Double.MAX_VALUE;
   double sumDividends = 0;
   //NYSEWritable reduceInValue = new NYSEWritable();
   StringBuilder sb = new StringBuilder("");
   int count = 0;
   reduceOutKey.set(reduceInKey.toString());
   for(NYSEWritable reduceInValue : reduceInValues){   
    
    if (reduceInValue.getFile_identifier().equals("prices")) {
     maxStockPrice = (maxStockPrice > reduceInValue
       .getStock_price_high()) ? maxStockPrice
       : reduceInValue.getStock_price_high();
     minStockPrice = (minStockPrice < reduceInValue
       .getStock_price_low()) ? minStockPrice
       : reduceInValue.getStock_price_low();
    } else if (reduceInValue.getFile_identifier().equals(
      "dividends")) {
     sumDividends = sumDividends
       + reduceInValue.getStock_dividend();
     count++;
    }
   }
   sb.append("companyName=");
   sb.append(companiesMap.get(reduceInKey.getStock_symbol().trim()));
   sb.append(",maxStockPrice=");
   sb.append(maxStockPrice);
   sb.append(",minStockPrice=");
   sb.append(minStockPrice);
   sb.append(",avgDividend=");
   sb.append(sumDividends / count);
   reduceOutValue.set(sb.toString());
   context.write(reduceOutKey, reduceOutValue);
   String filename=Integer.toString(reduceInKey.getStock_year()) + "/part";
   multipleOutputs.write(reduceOutKey, reduceOutValue, filename);
  }
 }

 /*
  * Customized class output file format class to split the output file of
  * reducer based on the Year. Refer to Point 6 in Design Considerations.
  */
//Defines additional single text based output 'text' for the job


 /* Main Driver Method */
 public static void main(String[] args) throws IOException,
   ClassNotFoundException, InterruptedException, Exception {
  //Configuration conf = new Configuration();
 //Job job = new Job(conf);
	 if (args == null || args.length < 5 || args[0] == null
				|| args[1] == null || args[2] == null || args[3] == null || args[4] == null) {
			System.out
					.println("Usage: hadoop jar nyse.jar <inputpath1> <inputpath2> <distcachefilepath> <outputpath> <delimiter>");
		}
		int result = ToolRunner.run(new Configuration(), new NYSEYearlyAnalysis(),
				args);
		System.exit(result);
  


 }

@Override
public int run(String[] arg0) throws Exception {
	// TODO Auto-generated method stub
	  Configuration configuration = getConf();
		// Set the delimiter and distributed cache file name as configuration
		// parameters
		configuration.set("job.distcache.delimiter", arg0[4]);
		configuration.set("job.distcache.cachefile", arg0[2]);
		// Get the instance of job
		Job job = Job.getInstance(configuration,
				"NYSEYearlyAnalysis");
		// Add distributed cache file to the job
		job.addCacheFile(new URI(arg0[2]));

	  job.setJobName("NYSE Yearly Analysis");

	  job.setJarByClass(NYSEYearlyAnalysis.class);
	  // Define Reducer Class. Mapper Classes are defined later for two diff
	  // input files.
	  job.setReducerClass(MyReducer.class);

	  // Output Key-Value Data types
	  job.setMapOutputKeyClass(NYSESymbolYearWritable.class);
	  job.setMapOutputValueClass(NYSEWritable.class);

	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);

	  // Inform Input/Output Formats and Directory Locations

	  /*
	   * Since there are two mappers here for two different files, You need to
	   * define which mapper processes which input file. Refer to Point 2 in
	   * Design Considerations.
	   */
	  MultipleInputs.addInputPath(job, new Path(arg0[0]),
	    TextInputFormat.class, PricesMapper.class);
	  MultipleInputs.addInputPath(job, new Path(arg0[1]),
	    TextInputFormat.class, DividendMapper.class);
	  FileOutputFormat.setOutputPath(job, new Path(arg0[3]));
	  LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
	  

	  // Inform termination criteria
	  boolean success = job.waitForCompletion(true);
	  System.exit(success ? 0 : 1);
	return 0;
}

}


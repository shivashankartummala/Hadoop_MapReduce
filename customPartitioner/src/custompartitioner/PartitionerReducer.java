package custompartitioner;



import java.io.IOException;
 
//import org.apache.hadoop.io.IntWritable;
 import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Reducer;
 
public class PartitionerReducer extends Reducer<Text, Text, Text, Text> {
 @Override
 protected void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException {
 
int maxSalary = Integer.MIN_VALUE;
 String name = " ";
 String age = " ";
 String gender = " ";
 int salary = 0;
 
//iterating through the values corresponding to a particular key
 for(Text val: values){
 String [] valTokens = val.toString().split("\\t");
 salary = Integer.parseInt(valTokens[2]);
 //if the new salary is greater than the current maximum salary, update the fields as they will be the output of the reducer after all the values are processed for a particular key
 
if(salary > maxSalary){
 name = valTokens[0];
 age = valTokens[1];
 gender = key.toString();
 maxSalary = salary;
 }
 }
 context.write(new Text(name), new Text("age- "+age+"\t"+gender+"\tscore-"+maxSalary));
 }
 }

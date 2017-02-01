package custompartitioner;


import org.apache.hadoop.io.Text;
 import org.apache.hadoop.mapreduce.Partitioner;
 
//AgePartitioner is a custom Partitioner to partition the data according to age.
 //The age is a part of the value from the input file.
 //The data is partitioned based on the range of the age.
 //In this example, there are 3 partitions, the first partition contains the information where the age is less than 20
 //The second partition contains data with age ranging between 20 and 50 and the third partition contains data where the age is >50.
 
public class AgePartitioner extends Partitioner<Text, Text> {
 @Override
 public int getPartition(Text key, Text value, int numReduceTasks) {
 
String [] nameAgeSalary = value.toString().split("\t");
 
String age = nameAgeSalary[1];
 
int ageInt = Integer.parseInt(age);
 //this is done to avoid performing mod with 0
 if(numReduceTasks == 0)
 return 0;
 //if the age is <20, assign partition 0
 if(ageInt <=20){
 return 0;
 }
 //else if the age is between 20 and 50, assign partition 1
 if(ageInt >20 && ageInt <=50){
 return 1 % numReduceTasks;
 }
 //otherwise assign partition 2
 else
 return 2 % numReduceTasks;
 
}
 
}

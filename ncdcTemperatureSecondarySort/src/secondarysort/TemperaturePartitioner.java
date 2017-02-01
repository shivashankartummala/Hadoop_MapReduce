package secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TemperaturePartitioner extends Partitioner<TemperaturePair, NullWritable>{
    @Override
    public int getPartition(TemperaturePair temperaturePair, NullWritable nullWritable, int numPartitions) {
        return temperaturePair.getYearMonth().hashCode() % numPartitions;
    }
}
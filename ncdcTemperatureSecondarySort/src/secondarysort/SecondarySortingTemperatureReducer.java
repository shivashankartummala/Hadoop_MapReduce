package secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class SecondarySortingTemperatureReducer extends Reducer<TemperaturePair, NullWritable, Text, IntWritable> {

    @Override
    protected void reduce(TemperaturePair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key.getYearMonth(), key.getTemperature());
    }
}

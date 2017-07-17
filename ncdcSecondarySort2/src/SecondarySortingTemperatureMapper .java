import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortingTemperatureMapper extends Mapper<LongWritable, Text, TemperaturePair, NullWritable> {

    private TemperaturePair temperaturePair = new TemperaturePair();
    private NullWritable nullValue = NullWritable.get();
    private static final int MISSING = 9999;
@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = value.split(",");
        // YYYY = tokens[0]
        // MM = tokens[1]
        // DD = tokens[2]
       // temperature = tokens[3]
    String yearMonth = tokens[0] + tokens[1];
    String day = tokens[2];
    int temperature = Integer.parseInt(tokens[3]);

        

        if (temp != MISSING) {
            temperaturePair.setYearMonth(yearMonth);
             temperaturePair.setDay(day);
            temperaturePair.setTemperature(temp);
            context.write(temperaturePair, nullValue);
        }
    }
}

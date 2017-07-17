import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearMonthGroupingComparator extends WritableComparator {
    public YearMonthGroupingComparator() {
        super(TemperaturePair.class, true);
    }
    @Override
    public int compare(WritableComparable tp1, WritableComparable tp2) {
        TemperaturePair temperaturePair = (TemperaturePair) tp1;
        TemperaturePair temperaturePair2 = (TemperaturePair) tp2;
        return temperaturePair.getYearMonth().compareTo(temperaturePair2.getYearMonth());
    }
}

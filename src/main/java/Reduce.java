import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jason on 3/2/14.
 */
public class Reduce extends Reducer<IntWritable, Weather, LongWritable, Text> {
    private LongWritable one = new LongWritable();
    //private IntWritable two = new IntWritable();
    private Text two = new Text();

    public void reduce(IntWritable key, Iterable<Weather> weather, Context context)
            throws IOException, InterruptedException {
        for (Weather w : weather) {
            one.set(w.getKey());
            int tid = context.getTaskAttemptID().getId();
            two.set(Integer.toString(w.getStation()) + "\t" + tid);

            context.write(one, two);
        }
        //context.write(one, text);
    }
}

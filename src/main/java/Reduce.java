import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jason on 3/2/14.
 */
public class Reduce extends Reducer<IntWritable, Weather, IntWritable, Text> {
    private Text text = new Text("one");
    private IntWritable one = new IntWritable(1);

    public void reduce(IntWritable key, Iterable<Weather> weather, Context context)
            throws IOException, InterruptedException {
            
        context.write(one, text);
    }
}

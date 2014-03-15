import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by jason on 3/2/14.
 */
public class Reduce extends Reducer<IntWritable, Weather, LongWritable, Text> {
    //private static final int dominates = 9;
    private static final int equivalent = 8;

    private LongWritable one = new LongWritable();
    //private IntWritable two = new IntWritable();
    private Text two = new Text();
    private ArrayList<Weather> skyline = new ArrayList<Weather>();

    public static int minComp(double node, double skyline) {
        if (node < skyline) {
            return 1;
        } else if (node > skyline) {
            return -1;
        } else {
            return 0;
        }
    }

    public static int maxComp(double node, double skyline) {
        if (node > skyline) {
            return 1;
        } else if (node < skyline) {
            return -1;
        } else {
            return 0;
        }
    }

    public void reduce(IntWritable key, Iterable<Weather> weather, Context context)
            throws IOException, InterruptedException {
        int dominates;
        for (Weather wOuter : weather) {
            if (this.skyline.isEmpty()) {
                this.skyline.add(wOuter);
            } else {
                for (Weather wInner : skyline) {
                    dominates = wOuter.compareTo(wInner);
                    int maxTemp = maxComp(wOuter.getTemp(), wInner.getTemp());
                    int maxDewp = maxComp(wOuter.getDewp(), wInner.getDewp());
                    int maxSlp = maxComp(wOuter.getSlp(), wInner.getSlp());
                    int minStp = minComp(wOuter.getStp(), wInner.getStp());
                    int minWdsp = minComp(wOuter.getWdsp(), wInner.getWdsp());
                    int maxMxspd = minComp(wOuter.getMxspd(), wInner.getMxspd());
                    int minGust = minComp(wOuter.getGust(), wInner.getGust());
                    int maxMax = maxComp(wOuter.getMax(), wInner.getMax());
                    int minMin = minComp(wOuter.getMin(), wInner.getMin());
                    //https://github.com/rweeks/util/blob/master/src/com/newbrightidea/util/RTree.java
                }
            }
        }
         /*
         for(Weather w : weather) {
            one.set(w.getKey());
            int tid = context.getTaskAttemptID().getId();
            two.set(Integer.toString(w.getStation()) + "\t" + tid);

            context.write(one, two);
         }*/
        //context.write(one, text);
    }

}

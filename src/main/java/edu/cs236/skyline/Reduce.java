package edu.cs236.skyline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.Map;

/**
 * Created by jason on 3/2/14.
 */
public class Reduce extends Reducer<LongWritable, Weather, LongWritable, Weather> {
    private LongWritable one = new LongWritable();
    //private IntWritable two = new IntWritable();
    private Text two = new Text();
    public String TAG = "reduce";
    private static final int dominates = 9;
    private static final int equals = 8;

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

    public void reduce(LongWritable key, Iterable<Weather> weather, Context context)
    throws IOException, InterruptedException {
        HashMap<Long, Weather> skylineMap = new HashMap<Long, Weather>();
        // Node array
        for (Weather nodes : weather) {
            Weather wOuter = new Weather();
            wOuter.copyObject(nodes);

            if (skylineMap.isEmpty()) {
                skylineMap.put(wOuter.getKey(), wOuter);
            } else {
                // Skyline array
                boolean addToSkyline = false;
                for (Map.Entry<Long, Weather> wInner : skylineMap.entrySet()) {
                    // Log.d(TAG, "Comparing: ");
                    // Log.d(TAG, "Node: " + wOuter.toString());
                    // Log.d(TAG, "Skyline: " + wInner.getValue().toString());
                    // Log.d(TAG, "====================================");

                    // 0 = equality
                    // 1 = domination
                    int[] skyline = new int[2];
                    skyline[0] = 0;
                    skyline[1] = 0;
                    int[] node = new int[2];
                    node[0] = 0;
                    node[1] = 0;

                    int maxTemp = maxComp(wOuter.getTemp(), wInner.getValue().getTemp());
                    int maxDewp = maxComp(wOuter.getDewp(), wInner.getValue().getDewp());
                    int maxSlp = maxComp(wOuter.getSlp(), wInner.getValue().getSlp());
                    int minStp = minComp(wOuter.getStp(), wInner.getValue().getStp());
                    int minWdsp = minComp(wOuter.getWdsp(), wInner.getValue().getWdsp());
                    int maxMxspd = minComp(wOuter.getMxspd(), wInner.getValue().getMxspd());
                    int minGust = minComp(wOuter.getGust(), wInner.getValue().getGust());
                    int maxMax = maxComp(wOuter.getMax(), wInner.getValue().getMax());
                    int minMin = minComp(wOuter.getMin(), wInner.getValue().getMin());
                    //https://github.com/rweeks/util/blob/master/src/com/newbrightidea/util/RTree.java

                    // Log.d(TAG, "MaxTemp: " + maxTemp);
                    if (maxTemp > 0) {
                        node[1]++;
                    } else if (maxTemp < 0) {
                        skyline[1]++;
                    } else {
                        node[0]++;
                        skyline[0]++;
                    }

                    // Log.d(TAG, "MaxDewp: " + maxDewp);
                    if (maxDewp > 0) {
                        node[1]++;
                    } else if (maxDewp < 0) {
                        skyline[1]++;
                    } else {
                        node[0]++;
                        skyline[0]++;
                    }

                    // Log.d(TAG, "MaxSLP: " + maxSlp);
                    if (maxSlp > 0) {
                        node[1]++;
                    } else if (maxSlp < 0) {
                        skyline[1]++;
                    } else {
                        node[0]++;
                        skyline[0]++;
                    }

                    // Log.d(TAG, "MinSTP: " + minStp);
                    if (minStp > 0) {
                        node[1]++;
                    } else if (minStp < 0) {
                        skyline[1]++;
                    } else {
                        node[0]++;
                        skyline[0]++;
                    }

                    // Log.d(TAG, "MinWdsp: " + minWdsp);
                    if (minWdsp > 0) {
                        node[1]++;
                    } else if (minWdsp < 0) {
                        skyline[1]++;
                    } else {
                        node[0]++;
                        skyline[0]++;
                    }

                    // Log.d(TAG, "MaxMxspd: " + maxMxspd);
                    if (maxMxspd > 0) {
                        node[1]++;
                    } else if (maxMxspd < 0) {
                        skyline[1]++;
                    } else {
                        node[0]++;
                        skyline[0]++;
                    }

                    // Log.d(TAG, "MinGust: " + minGust);
                    if (minGust > 0) {
                        node[1]++;
                    } else if (minGust < 0) {
                        skyline[1]++;
                    } else {
                        node[0]++;
                        skyline[0]++;
                    }

                    // Log.d(TAG, "MaxMax: " + maxMax);
                    if (maxMax > 0) {
                        node[1]++;
                    } else if (maxMax < 0) {
                        skyline[1]++;
                    } else {
                        node[0]++;
                        skyline[0]++;
                    }

                    // Log.d(TAG, "MinMin: " + minMin);
                    if (minMin > 0) {
                        node[1]++;
                    } else if (minMin < 0) {
                        skyline[1]++;
                    } else {
                        node[0]++;
                        skyline[0]++;
                    }

                    // The node is dominating the skyline
                    if (node[0] == equals && node[1] == 1) {
                        // remove the current object
                        skylineMap.remove(wInner.getKey());
                        addToSkyline = true;
                        break;
                    }
                    // the node dominates in all attributes
                    else if (node[1] == dominates) {
                        // remove the current object
                        skylineMap.remove(wInner.getKey());
                        addToSkyline = true;
                        break;
                    }
                    // the skyline dominates in one attribute
                    else if (skyline[0] == equals && skyline[1] == 1) {
                        break;
                    }
                    // the skyline dominates
                    else if (skyline[1] == dominates) {
                        break;
                    }
                    // The node belongs in the skyline,
                    // it dominates in one or more attributes
                    else {
                        addToSkyline = true;
                        break;
                    }
                }// skyline
                if (addToSkyline) {
                    skylineMap.put(wOuter.getKey(), wOuter);
                }
                // Log.d(TAG, "====================================");
            }
        } // for nodes
        //context.write(one, text);
        for (Map.Entry<Long, Weather> w : skylineMap.entrySet()) {
            one.set(w.getKey());
            Log.d(TAG, "Writing: " + w.getValue().getKey());
            context.write(one, w.getValue());//skylineMap.get(w.getKey()));
        }
    }

}

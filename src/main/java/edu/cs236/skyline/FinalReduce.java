package edu.cs236.skyline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jason on 3/19/14.
 */
public class FinalReduce extends Reducer<LongWritable, Weather, Text, Weather> {

    //private LongWritable one = new LongWritable();
    //private IntWritable two = new IntWritable();
    private Text one = new Text();
    public String TAG = "reduce";

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
        java.util.Map<Long, Weather> skylineMap = new ConcurrentHashMap<Long, Weather>();
        // Node array
        int i = 0;
        for (Weather nodes : weather) {
            Weather wOuter = new Weather();
            wOuter.copyObject(nodes);
            if (skylineMap.isEmpty()) {
                skylineMap.put(wOuter.getKey(), wOuter);
            } else {
                // Skyline array
                boolean addToSkyline = false;
                for (java.util.Map.Entry<Long, Weather> wInner : skylineMap.entrySet()) {
                    // Log.d(TAG, "Comparing: ");
                    // Log.d(TAG, "Node: " + wOuter.toString());
                    // Log.d(TAG, "Skyline: " + wInner.getValue().toString());
                    // Log.d(TAG, "====================================");
                    // 0 = equality
                    // [n] = 1 = domination

                    int skyline = 0;
                    int node = 0;
                    // equality array
                    //0 = skyline
                    //1 = node
                    int eq = 0;

                    int maxTemp = maxComp(wOuter.getTemp(), wInner.getValue().getTemp());
                    int maxDewp = maxComp(wOuter.getDewp(), wInner.getValue().getDewp());
                    int maxSlp = maxComp(wOuter.getSlp(), wInner.getValue().getSlp());
                    int minStp = minComp(wOuter.getStp(), wInner.getValue().getStp());
                    int minWdsp = minComp(wOuter.getWdsp(), wInner.getValue().getWdsp());
                    int maxMxspd = minComp(wOuter.getMxspd(), wInner.getValue().getMxspd());
                    int minGust = minComp(wOuter.getGust(), wInner.getValue().getGust());
                    int maxMax = maxComp(wOuter.getMax(), wInner.getValue().getMax());
                    int minMin = minComp(wOuter.getMin(), wInner.getValue().getMin());

                    // Log.d(TAG, "MaxTemp: " + maxTemp);
                    if (maxTemp > 0) {
                        node++;
                    } else if (maxTemp < 0) {
                        skyline++;
                    } else {
                        eq++;
                    }

                    // Log.d(TAG, "MaxDewp: " + maxDewp);
                    if (maxDewp > 0) {
                        node++;
                    } else if (maxDewp < 0) {
                        skyline++;
                    } else {
                        eq++;
                    }

                    // Log.d(TAG, "MaxSLP: " + maxSlp);
                    if (maxSlp > 0) {
                        node++;
                    } else if (maxSlp < 0) {
                        skyline++;
                    } else {
                        eq++;
                    }

                    // Log.d(TAG, "MinSTP: " + minStp);
                    if (minStp > 0) {
                        node++;
                    } else if (minStp < 0) {
                        skyline++;
                    } else {
                        eq++;
                    }

                    // Log.d(TAG, "MinWdsp: " + minWdsp);
                    if (minWdsp > 0) {
                        node++;
                    } else if (minWdsp < 0) {
                        skyline++;
                    } else {
                        eq++;
                    }

                    // Log.d(TAG, "MaxMxspd: " + maxMxspd);
                    if (maxMxspd > 0) {
                        node++;
                    } else if (maxMxspd < 0) {
                        skyline++;
                    } else {
                        eq++;
                    }

                    // Log.d(TAG, "MinGust: " + minGust);
                    if (minGust > 0) {
                        node++;
                    } else if (minGust < 0) {
                        skyline++;
                    } else {
                        eq++;
                    }

                    // Log.d(TAG, "MaxMax: " + maxMax);
                    if (maxMax > 0) {
                        node++;
                    } else if (maxMax < 0) {
                        skyline++;
                    } else {
                        eq++;
                    }

                    // Log.d(TAG, "MinMin: " + minMin);
                    if (minMin > 0) {
                        node++;
                    } else if (minMin < 0) {
                        skyline++;
                    } else {
                        eq++;
                    }

                    // The node is dominating the skyline and everything is equal or less than

                    if (skyline == 0 && node > 0) {
                        // remove the current object
                        skylineMap.remove(wInner.getKey());
                        addToSkyline = true;
                        i--;
                        if (node == 1) break;
                    }
                    // The node is equal to or less than skyline
                    else if (node == 0) {
                        addToSkyline = false;
                        // remove the current object
                        break;
                    } else {
                        addToSkyline = true;
                    }
                }// skyline
                if (addToSkyline) {
                    skylineMap.put(wOuter.getKey(), wOuter);
                    i++;
                }
                // Log.d(TAG, "====================================");
            }
        } // for nodes
        //context.write(one, text);
        for (java.util.Map.Entry<Long, Weather> w : skylineMap.entrySet()) {
            String id = w.getValue().getStation() + "_" + w.getValue().getYear() + "_" + w.getValue().getModa();
            one.set(id);
            context.write(one, w.getValue());
        }
    }


}

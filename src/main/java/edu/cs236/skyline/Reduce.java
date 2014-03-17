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
    //private static final int dominates = 9;
    private static final int equivalent = 8;

    private LongWritable one = new LongWritable();
    //private IntWritable two = new IntWritable();
    private Text two = new Text();
    private HashMap<Long, Weather> skylineMap = new HashMap<Long, Weather>();

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
        // Node array
        for (Weather nodes : weather) {
            Weather wOuter = new Weather();
            wOuter.copyObject(nodes);

            if (this.skylineMap.isEmpty()) {
                this.skylineMap.put(wOuter.getKey(), wOuter);
            } else {
                // Skyline array
                boolean addToSkyline = false;
                for (Map.Entry<Long, Weather> wInner : skylineMap.entrySet()) {
                    Log.d("Comparing: ");
                    Log.d("Node: " + wOuter.toString());
                    Log.d("Skyline: " + wInner.getValue().toString());
                    Log.d("====================================");
                    int skyline = 0;
                    int node = 0;

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

                    Log.d("MaxTemp: " + maxTemp);
                    if (maxTemp <= 0) {
                        skyline++;
                    } else {
                        node++;
                    }

                    Log.d("MaxDewp: " + maxDewp);
                    if (maxDewp <= 0) {
                        skyline++;
                    } else {
                        node++;
                    }

                    Log.d("MaxSLP: " + maxSlp);
                    if (maxSlp <= 0) {
                        skyline++;
                    } else {
                        node++;
                    }

                    Log.d("MinSTP: " + minStp);
                    if (minStp <= 0) {
                        skyline++;
                    } else {
                        node++;
                    }

                    Log.d("MinWdsp: " + minWdsp);
                    if (minWdsp <= 0) {
                        skyline++;
                    } else {
                        node++;
                    }

                    Log.d("MaxMxspd: " + maxMxspd);
                    if (maxMxspd <= 0) {
                        skyline++;
                    } else {
                        node++;
                    }

                    Log.d("MinGust: " + minGust);
                    if (minGust <= 0) {
                        skyline++;
                    } else {
                        node++;
                    }

                    Log.d("MaxMax: " + maxMax);
                    if (maxMax <= 0) {
                        skyline++;
                    } else {
                        node++;
                    }

                    Log.d("MinMin: " + minMin);
                    if (minMin <= 0) {
                        skyline++;
                    } else {
                        node++;
                    }

                    if (node == 0) {
                        break;
                    } else if (skyline == 0) {
                        addToSkyline = true;
                        skylineMap.remove(wInner.getKey());
                        break;
                    }
                    addToSkyline = true;
                    /*
                    if (node > skyline) {
                        if (!skylineMap.containsKey(wOuter.getKey()))
                            skylineMap.put(wOuter.getKey(), wOuter);
                    }*/
                }// skyline
                if (addToSkyline) {
                    skylineMap.put(wOuter.getKey(), wOuter);
                }
                Log.d("====================================");
            }
        } // for nodes
         /*
         for(Weather w : weather) {
            one.set(w.getKey());
            int tid = context.getTaskAttemptID().getId();
            two.set(Integer.toString(w.getStation()) + "\t" + tid);

            context.write(one, two);
         }*/
        //context.write(one, text);
        for (Map.Entry<Long, Weather> w : skylineMap.entrySet()) {
            one.set(w.getKey());
            context.write(one, skylineMap.get(w.getKey()));
        }
    }

}

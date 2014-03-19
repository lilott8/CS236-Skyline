package edu.cs236.skyline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jason on 3/2/14.
 */
public class InitMap extends Mapper<LongWritable, Text, LongWritable, Weather> {

    private LongWritable id = new LongWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // get the number of reducertasks
        int reducers = context.getNumReduceTasks();
        // This is just a key to generate a unique key
        long newKey = Skyline.getKey();
        // generate the modulus for our weather object
        long mod = newKey % Singleton.Modulus.getMod();

        if (line.length() > 108) {
            /**
             * The file assumes a starting point of 1
             * I need the following attributes from the file:
             * StationID:   1-6     Int
             * Year:        15-18   Int
             * Moda:        19-22   Double
             * Temp:        25-30   Double
             * Dewp:        36-41   Double
             * SLP:         47-52   Double
             * Max:         103-108 Double
             * STP:         58-63   Double
             * WDSP:        79-83   Double
             * MXSPD:       89-93   Double
             * Gust:        96-100  Double
             * Min:         111-116 Double
             */

            // create the object to hold our data
            Weather w = new Weather();
            // write the id to our LongWritable
            id.set(mod);
            // Attributes of our weather object
            w.setKey(newKey);
            w.setStation(line.substring(0, 6).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setYear(line.substring(14, 18).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setModa(line.substring(18, 22).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setTemp(line.substring(24, 30).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setDewp(line.substring(35, 41).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setSlp(line.substring(46, 52).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setMax(line.substring(102, 108).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setStp(line.substring(57, 63).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setWdsp(line.substring(78, 83).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setMxspd(line.substring(88, 93).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setGust(line.substring(95, 100).replaceAll("\\s+", "").replaceAll("\\*", ""));
            w.setMin(line.substring(110, 116).replaceAll("\\s+", "").replaceAll("\\*", ""));

            // push the object onto our data structure
            context.write(id, w);
        }// if line>108
    }
}// class

package edu.cs236.skyline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jason on 3/2/14.
 */
public class Map extends Mapper<LongWritable, Text, LongWritable, Weather> {

    private LongWritable id = new LongWritable();
    public String TAG = "map";

    public void map(LongWritable key, Text weather, Context context) throws IOException, InterruptedException {
        String weatherString = weather.toString();
        // create the object to hold our data
        Weather w = new Weather();
        Log.d(TAG, "Attempting to parse: " + weather);
        // write the id to our LongWritable
        //id.set(mod);

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

        /*
        this.key = in.readLong();
        this.station = in.readInt();
        this.year = in.readInt();
        this.moda = in.readInt();
        this.temp = in.readDouble();
        this.dewp = in.readDouble();
        this.slp = in.readDouble();
        this.max = in.readDouble();
        this.stp = in.readDouble();
        this.wdsp = in.readDouble();
        this.mxspd = in.readDouble();
        this.gust = in.readDouble();
        this.min = in.readDouble();

        */

        int mod = Integer.parseInt(context.getConfiguration().get("mod"));
        System.out.println(mod);
        List<String> values = new ArrayList<String>(Arrays.asList(weatherString.split("\\|")));
        //Log.d(TAG, "Size of values: " + values.size());
        //Log.d(TAG, "Parsed:" + weatherString + " into array");
        // Attributes of our weather object
        List<String> theKeys = Arrays.asList(values.get(0).split("\\s+"));
        //Log.d(TAG, "Parsed key: " + theKeys.get(1));
        w.setKey(Long.parseLong(theKeys.get(1)));
        //Log.d(TAG, "Parsed station: " + w.getStation());
        w.setStation(values.get(1));
        //Log.d(TAG, "Parsed station: " + values.get(2));
        w.setYear(values.get(2));
        //Log.d(TAG, "Parsed station: " + values.get(3));
        w.setModa(values.get(3));
        //Log.d(TAG, "Parsed station: " + values.get(4));
        w.setTemp(values.get(4));
        //Log.d(TAG, "Parsed station: " + values.get(5));
        w.setDewp(values.get(5));
        w.setSlp(values.get(6));
        w.setMax(values.get(8));
        w.setStp(values.get(7));
        w.setWdsp(values.get(9));
        w.setMxspd(values.get(10));
        w.setGust(values.get(11));
        w.setMin(values.get(12));

        //w.copyObject(weather);
        id.set(w.getKey() % mod);
        context.write(id, w);
    }// map
}// class



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jason on 3/2/14.
 */
public class Map extends Mapper<LongWritable, Text, IntWritable, Weather> {

    private IntWritable id = new IntWritable(1);
    private Weather we = new Weather();
    //private IntWritable we = new IntWritable(2);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s;
        String line = value.toString();
        //int start[] =   {0,18,31,42,53,64,74,84,88,103};
        //int end[] =     {6,22,33,44,55,66,76,86,93,108};

        if (line.length() > 108) {
            // create the object to hold our data
            Weather w = new Weather();

            // parse the string
            s = line.substring(0, 6).replaceAll("\\s+", "");
            id.set(Integer.parseInt(s));
            w.setStation(Integer.parseInt(s));

            s = line.substring(18, 22).replaceAll("\\s+", "");
            w.setModa(Integer.parseInt(s)); /** Attribute 4, 19-22 **/ /** Min **/

            s = line.substring(31, 33).replaceAll("\\s+", "");
            w.setCountTemp(Double.parseDouble(s));/** Attribute 6, 32-33 **/ /** Min **/

            s = line.substring(42, 44).replaceAll("\\s+", "");
            w.setCountDewp(Double.parseDouble(s));/** Attribute 8, 43-44 **/ /** Min **/

            s = line.substring(53, 55).replaceAll("\\s+", "");
            w.setCountSlp(Integer.parseInt(s));/** Attribute 10, 54-55 **/ /** Max **/

            s = line.substring(64, 66).replaceAll("\\s+", "");
            w.setCountStp(Integer.parseInt(s));/** Attribute 12, 65-66 **/ /** Min **/

            s = line.substring(74, 76).replaceAll("\\s+", "");
            w.setCountVisib(Integer.parseInt(s));/** Attribute 14, 75-76 **/ /** Max **/

            s = line.substring(84, 86).replaceAll("\\s+", "");
            w.setCountWdsp(Integer.parseInt(s));/** Attribute 16, 85-86 **/ /** Max **/

            s = line.substring(88, 93).replaceAll("\\s+", "");
            w.setMxspd(Double.parseDouble(s));/** Attribute 17, 89-93 **/ /** Max **/

            s = line.substring(103, 108).replaceAll("\\s+", "");
            if (s.contains("*"))
                s = s.substring(0, s.indexOf("*"));
            w.setMax(Double.parseDouble(s));/** Attribute 19, 103-108 **/ /** Max **/

            // push the object onto our data structure
            context.write(id, we);
        }// if line>108
    }
}// class

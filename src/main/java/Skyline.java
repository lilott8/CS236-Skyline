import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by jason on 2/26/14.
 */

/**
 * https://github.com/fsi206914/Skydoop
 */
public class Skyline {

    public static void main(String[] args) throws IOException {
        //String s = args[0].length() > 0 ? args[0] : "skyline.in";
        Path input, output;
        Configuration conf = new Configuration();

        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");
        try {
            input = new Path(args[0]);
        } catch (ArrayIndexOutOfBoundsException e) {
            input = new Path("hdfs://localhost/user/cloudera/in/skyline.in");
        }
        try {
            output = new Path(args[1]);
            //FileSystem.getLocal(conf).delete(output, true);;
        } catch (ArrayIndexOutOfBoundsException e) {
            output = new Path("hdfs://localhost.localdomain/user/cloudera/out/");
            //FileSystem.getLocal(conf).delete(output, true);
        }

        Job job = new Job(conf, "skyline");

        job.setJarByClass(Skyline.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Weather.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            System.out.println("Interrupted Exception");
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException");
        }
    }
}

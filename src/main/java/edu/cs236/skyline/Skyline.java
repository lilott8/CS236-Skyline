package edu.cs236.skyline;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by jason on 2/26/14
 */

/**
 * https://github.com/fsi206914/Skydoop
 */
public class Skyline {

    public static String TAG = "skyline";
    private static long key = 0;
    // we want this to start at 50k for 11000000 records
    private static int mod = 1000;

    public static synchronized long getKey() {
        return key++;
    }

    public static synchronized int getMod() {
        return mod;
    }

    public static synchronized void setMod(int m) {
        mod = mod / m;
    }

    public static void main(String[] args) throws IOException {
        //String s = args[0].length() > 0 ? args[0] : "skyline.in";
        Path input, output;
        int reducers;
        int x = 0;

        Configuration conf = new Configuration();

        conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");

        try {
            reducers = Integer.parseInt(args[0]);
        } catch (ArrayIndexOutOfBoundsException e) {
            reducers = 1;
        }
        try {
            input = new Path(args[1]);
        } catch (ArrayIndexOutOfBoundsException e) {
            input = new Path("hdfs://localhost/user/cloudera/in/tenThousand");
            // input = new Path("hdfs://localhost/user/cloudera/in/skyline.in");
        }

        while (getMod() >= 1) {

            Log.d(TAG, "getMod: " + getMod() + "\tx: " + x);
            Job job = new Job(conf, "skyline");

            job.setJarByClass(Skyline.class);

            try {
                output = new Path(args[2] + x);
            } catch (ArrayIndexOutOfBoundsException e) {
                output = new Path("hdfs://localhost.localdomain/user/cloudera/out/" + x);
            }

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Weather.class);

            if (x == 0) {
                job.setMapperClass(InitMap.class);
            } else {
                job.setMapperClass(Map.class);
            }
            job.setReducerClass(Reduce.class);

            //job.setInputFormatClass(TextInputFormat.class);
            //job.setOutputFormatClass(TextOutputFormat.class);

            job.setNumReduceTasks(reducers);

            FileInputFormat.addInputPath(job, input);
            FileOutputFormat.setOutputPath(job, output);

            try {
                job.waitForCompletion(true);
            } catch (InterruptedException e) {
                System.out.println("Interrupted Exception");
            } catch (ClassNotFoundException e) {
                System.out.println("ClassNotFoundException");
            }

            try {
                input = new Path(args[2] + x);
            } catch (ArrayIndexOutOfBoundsException e) {
                input = new Path("hdfs://localhost/user/cloudera/out/" + x);
                //input = new Path("hdfs://localhost/user/cloudera/in/skyline.in");
            }

            setMod(10);
            x++;

            /*if (mod == 1) {
                mod--;
            }*/
        }

        /*
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Weather.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(reducers);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        try {
            job.waitForCompletion(true);
        } catch (InterruptedException e) {
            System.out.println("Interrupted Exception");
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFoundException");
        }
        */
    }
}

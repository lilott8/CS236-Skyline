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
    private static int mod;
    public static int modder;

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

        // Number of reducer tasks
        try {
            if ((reducers = Integer.parseInt(args[0])) < 1) {
                reducers = 1;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            reducers = 1;
        }
        // Modulus
        try {
            if ((mod = Integer.parseInt(args[1])) < 1) {
                mod = 1000;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            mod = 1000;
        }
        System.out.println("mod is: " + mod);
        // modder
        try {
            if ((modder = Integer.parseInt(args[2])) < 1) {
                modder = 10;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            modder = 10;
        }
        System.out.println("Modder is; " + modder);

        // Path to input
        try {
            input = new Path(args[3]);
        } catch (ArrayIndexOutOfBoundsException e) {
            input = new Path("hdfs://localhost/user/cloudera/in/tiny");
            // input = new Path("hdfs://localhost/user/cloudera/in/skyline.in");
        }

        while (getMod() >= 1) {

            conf.set("mod", Integer.toString(mod));
            Job job = new Job(conf, "skyline");

            job.setJarByClass(Skyline.class);

            try {
                output = new Path(args[4] + x);
            } catch (ArrayIndexOutOfBoundsException e) {
                //Log.d(TAG, "output: hdfs://localhost/user/cloudera/out/"+x);
                output = new Path("hdfs://localhost.localdomain/user/cloudera/out/" + x);
            }

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Weather.class);

            if (x == 0) {
                job.setMapperClass(InitMap.class);
            } else {
                job.setMapperClass(Map.class);
            }

            if ((getMod() / modder) >= 1) {
                job.setReducerClass(Reduce.class);
            } else {
                job.setReducerClass(FinalReduce.class);
            }

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
                input = new Path(args[4] + x);
            } catch (ArrayIndexOutOfBoundsException e) {
                //Log.d(TAG, "input: hdfs://localhost/user/cloudera/out/"+x);
                input = new Path("hdfs://localhost/user/cloudera/out/" + x);
                //input = new Path("hdfs://localhost/user/cloudera/in/skyline.in");
            }

            setMod(modder);
            x++;
        }
    }
}

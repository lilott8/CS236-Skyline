package edu.cs236.skyline;

/**
 * Created by jason on 3/17/14.
 */
public abstract class Log {

    public static boolean print = true;

    public static void d(String tag, String message) {
        String out = String.format("%s:\t%s", tag, message);
        if (print) {
            System.out.println(out);
        }
    }

    public static void d(String message) {
        String out = String.format("Map/Reduce:\t%s", message);
        if (print) {
            System.out.println(out);
        }
    }

}

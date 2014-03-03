import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jason on 2/26/14.
 */
public class Weather implements Writable {

    private int station;
    /**
     * Attribute 1, 1-6
     */
    private int wban;
    /**
     * Attribute 2, 8-12 *
     */
    private int year;
    /**
     * Attribute 3, 15-18 *
     */
    private int moda;/** Attribute 4, 19-22 **/
    /**
     * Min *
     */
    private double temp;
    /**
     * Attribute 5, 25-30 *
     */
    private double countTemp;/** Attribute 6, 32-33 **/
    /**
     * Min *
     */
    private double dewp;
    /**
     * Attribute 7, 36-41 *
     */
    private double countDewp;/** Attribute 8, 43-44 **/
    /**
     * Min *
     */
    private double slp;
    /**
     * Attribute 9, 47-52 *
     */
    private int countSlp;/** Attribute 10, 54-55 **/
    /**
     * Max *
     */
    private double stp;
    /**
     * Attribute 11, 58-63 *
     */
    private int countStp;/** Attribute 12, 65-66 **/
    /**
     * Min *
     */
    private double visib;
    /**
     * Attribute 13, 69-73 *
     */
    private int countVisib;/** Attribute 14, 75-76 **/
    /**
     * Max *
     */
    private double wdsp;
    /**
     * Attribute 15, 79-83 *
     */
    private int countWdsp;/** Attribute 16, 85-86 **/
    /**
     * Max *
     */
    private double mxspd;/** Attribute 17, 89-93 **/
    /**
     * Max *
     */
    private double gust;
    /**
     * Attribute 18, 96-100 *
     */
    private double max;/** Attribute 19, 103-108 **/
    /**
     * Max *
     */
    private char flagMaxTemp;
    /**
     * Attribute 20, 109 *
     */
    private double min;
    /**
     * Attribute 21, 111-116 *
     */
    private char flaxMin;
    /**
     * Attribute 22, 117 *
     */
    private double prcp;
    /**
     * Attribute 23, 119-123 *
     */
    private char flagPrcp;
    /**
     * Attribute 24, 124 *
     */
    private double sndp;
    /**
     * Attribute 25, 126-130 *
     */
    private int frshtt;

    /**
     * Attribute 26, 133-138 *
     */

    public Weather() {
    }

    public void setStation(int m) {
        this.station = m;
    }

    public void setModa(int m) {
        this.moda = m;
    }

    public void setCountTemp(double m) {
        this.countTemp = m;
    }

    public void setCountDewp(double m) {
        this.countDewp = m;
    }

    public void setCountSlp(int m) {
        this.countSlp = m;
    }

    public void setCountStp(int m) {
        this.countStp = m;
    }

    public void setCountVisib(int m) {
        this.countVisib = m;
    }

    public void setCountWdsp(int m) {
        this.countWdsp = m;
    }

    public void setMxspd(double m) {
        this.mxspd = m;
    }

    public void setMax(double m) {
        this.max = m;
    }

    public int getStation() {
        return this.station;
    }

    public int getModa() {
        return this.moda;
    }

    public double getCountTemp() {
        return this.countTemp;
    }

    public double getCountDewp() {
        return this.countDewp;
    }

    public int getCountSlp() {
        return this.countSlp;
    }

    public int getCountStp() {
        return this.countStp;
    }

    public int getCountVisib() {
        return this.countVisib;
    }

    public int getCountWdsp() {
        return this.countWdsp;
    }

    public double getMxspd() {
        return this.mxspd;
    }

    public double getMax() {
        return this.max;
    }

    public String toString() {
        return String.format("Station:\t%d\nModa:\t%d\nTemp:\t%f\nDewp:\t%f\n" +
                "Slp:\t%d\nStp:\t%d\nVisib:\t%d\nWdsp:\t%d\nMxsp:\t%f\nMax:\t%f\n" +
                "***************************************\n",
                this.station, this.moda, this.countTemp, this.countDewp,
                this.countSlp, this.countStp, this.countVisib, this.countWdsp,
                this.mxspd, this.max);
    }

    public static Weather read(DataInput in) throws IOException {
        Weather w = new Weather();
        w.readFields(in);
        return w;
    }

    /**
     * Mandatory function for implementing writable
     *
     * @param in datainput from the mapreduce framework
     * @throws IOException in case we can't read the file
     */
    public void readFields(DataInput in) throws IOException {
        this.station = in.readInt();
        this.moda = in.readInt();
        this.countTemp = in.readDouble();
        this.countDewp = in.readDouble();
        this.countSlp = in.readInt();
        this.countStp = in.readInt();
        this.countVisib = in.readInt();
        this.countWdsp = in.readInt();
        this.mxspd = in.readDouble();
        this.max = in.readDouble();
    }

    /**
     * Mandatory function for implementing writable
     *
     * @param out object that allows us to write to disk
     * @throws IOException in case we can't write to disk
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.station);
        out.writeInt(this.moda);
        out.writeDouble(this.countTemp);
        out.writeDouble(this.countDewp);
        out.writeInt(this.countSlp);
        out.writeInt(this.countStp);
        out.writeInt(this.countVisib);
        out.writeInt(this.countWdsp);
        out.writeDouble(this.mxspd);
        out.writeDouble(this.max);
    }

}

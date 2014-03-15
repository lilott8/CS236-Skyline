import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jason on 2/26/14.
 */
public class Weather implements Writable {

    private long key;

    /**
     * Attribute 1, 1-6
     */
    private int station; /* KEY */

    /**
     * Attribute 2, 8-12
     */
    private int wban;

    /**
     * Attribute 3, 15-18
     */
    private int year; /* KEY */

    /**
     * Attribute 4, 19-22
     */
    private int moda;/* KEY */

    /**
     * Attribute 5, 25-30
     */
    private double temp; /* MAX */

    /**
     * Attribute 6, 32-33
     **/
    private double countTemp;

    /**
     * Attribute 7, 36-41
     */
    private double dewp; /* MAX */

    /**
     * Attribute 8, 43-44
     */
    private double countDewp;

    /**
     * Attribute 9, 47-52
     */
    private double slp; /* MAX */

    /**
     * Attribute 10, 54-55
     **/
    private int countSlp;

    /**
     * Attribute 11, 58-63
     */
    private double stp; /* MIN */

    /**
     * Attribute 12, 65-66
     **/
    private int countStp;

    /**
     * Attribute 13, 69-73
     */
    private double visib;

    /**
     * Attribute 14, 75-76
     **/
    private int countVisib;

    /**
     * Attribute 15, 79-83
     */
    private double wdsp; /* MIN */

    /**
     * Attribute 16, 85-86
     */
    private int countWdsp;

    /**
     * Attribute 17, 89-93
     */
    private double mxspd;/* MIN */

    /**
     * Attribute 18, 96-100
     */
    private double gust; /* MIN */

    /**
     * Attribute 19, 103-108
     */
    private double max;

    /**
     * Attribute 20, 109
     */
    private char flagMaxTemp;

    /**
     * Attribute 21, 111-116
     */
    private double min; /* MIN */

    /**
     * Attribute 22, 117-117
     */
    private char flagMin;

    /**
     * Attribute 23, 119-123
     */
    private double prcp;

    /**
     * Attribute 24, 124
     */
    private char flagPrcp;

    /**
     * Attribute 25, 126-130
     */
    private double sndp;

    /**
     * Attribute 26, 133-138
     */
    private int frshtt;

    public Weather() {
    }

    public void setKey(long k) {
        this.key = k;
    }

    public void setStation(String m) {
        this.station = Integer.parseInt(m);
    }

    public void setYear(String m) {
        this.year = Integer.parseInt(m);
    }

    public void setModa(String m) {
        this.moda = Integer.parseInt(m);
    }

    public void setTemp(String m) {
        this.temp = Double.parseDouble(m);
    }

    public void setCountTemp(String m) {
        this.countTemp = Double.parseDouble(m);
    }

    public void setDewp(String m) {
        this.dewp = Double.parseDouble(m);
    }

    public void setCountDewp(String m) {
        this.countDewp = Double.parseDouble(m);
    }

    public void setSlp(String m) {
        this.slp = Double.parseDouble(m);
    }

    public void setCountSlp(String m) {
        this.countSlp = Integer.parseInt(m);
    }

    public void setStp(String m) {
        this.stp = Double.parseDouble(m);
    }

    public void setCountStp(String m) {
        this.countStp = Integer.parseInt(m);
    }

    public void setVisib(String m) {
        this.visib = Double.parseDouble(m);
    }

    public void setCountVisib(String m) {
        this.countVisib = Integer.parseInt(m);
    }

    public void setWdsp(String m) {
        this.wdsp = Double.parseDouble(m);
    }

    public void setCountWdsp(String m) {
        this.countWdsp = Integer.parseInt(m);
    }

    public void setMxspd(String m) {
        this.mxspd = Double.parseDouble(m);
    }

    public void setMax(String m) {
        this.max = Double.parseDouble(m);
    }

    public void setMin(String m) {
        this.min = Double.parseDouble(m);
    }

    public void setGust(String m) {
        this.gust = Double.parseDouble(m);
    }

    /**
     * The file assumes a starting point of 1
     * I need the following attributes from the file:
     * StationID: 1-6
     * Year: 15-18
     * Moda: 19-22
     * Temp: 25-30
     * Dewp: 36-41
     * SLP: 47-52
     * Max: 103-108
     * STP: 58-63
     * WDSP: 79-83
     * MXSPD: 89-93
     * Gust: 96-100
     * Min: 111-116
     */

    public long getKey() {
        return this.key;
    }

    public int getStation() {
        return this.station;
    }

    public int getYear() {
        return this.year;
    }

    public int getModa() {
        return this.moda;
    }

    public double getTemp() {
        return this.temp;
    }

    public double getDewp() {
        return this.dewp;
    }

    public double getSlp() {
        return this.slp;
    }

    public double getMax() {
        return this.max;
    }

    public double getStp() {
        return this.stp;
    }

    public double getWdsp() {
        return this.wdsp;
    }

    public double getMxspd() {
        return this.mxspd;
    }

    public double getGust() {
        return this.gust;
    }

    public double getMin() {
        return this.min;
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
    }

    /**
     * Mandatory function for implementing writable
     *
     * @param out object that allows us to write to disk
     * @throws IOException in case we can't write to disk
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.key);
        out.writeInt(this.station);
        out.writeInt(this.year);
        out.writeInt(this.moda);
        out.writeDouble(this.temp);
        out.writeDouble(this.dewp);
        out.writeDouble(this.slp);
        out.writeDouble(this.max);
        out.writeDouble(this.stp);
        out.writeDouble(this.wdsp);
        out.writeDouble(this.mxspd);
        out.writeDouble(this.gust);
        out.writeDouble(this.min);
    }

}

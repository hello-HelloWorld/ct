package kv.key;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/20 14:23
 */

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//时间维度，用于对比时间
public class DateDimension extends BaseDimension {
    private String year;
    private String month;
    private String day;

    public DateDimension() {
        super();
    }

    public DateDimension(String year, String month, String day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DateDimension other = (DateDimension) obj;
        if (year != null ? !year.equals(other.year) : other.year != null) {
            return false;
        }
        if (month != null ? !month.equals(other.month) : other.month != null) {
            return false;
        }
        return day != null ? day.equals(other.day) : other.day == null;
    }

    @Override
    public int hashCode() {
        int result = year != null ? year.hashCode() : 0;
        result = 31 * result + (month != null ? month.hashCode() : 0);
        result = 31 * result + (day != null ? day.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(BaseDimension o) {
        DateDimension anotherDateDimension = (DateDimension) o;
        int result = this.year.compareTo(anotherDateDimension.year);
        if (result != 0) {
            return result;
        }
        result = this.month.compareTo(anotherDateDimension.month);
        if (result != 0) {
            return result;
        }
        result = this.day.compareTo(anotherDateDimension.day);
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.year);
        dataOutput.writeUTF(this.month);
        dataOutput.writeUTF(this.day);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readUTF();
        this.month = dataInput.readUTF();
        this.day = dataInput.readUTF();
    }
}

package maxTemp.allsortsecondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 组合key
 * @author LaZY（李志一）
 * @data 2019/1/8 - 19:01
 */
public class ComboKey implements WritableComparable<ComboKey>{
    private int year;
    private int temp;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    /***
     * 对key进行比较实现
     * @param o
     * @return
     */
    public int compareTo(ComboKey o) {
        int y0 = o.getYear();
        int t0 = o.getTemp();
        if(year == y0){//年升序
            return -(temp - t0);
        }else {//气温降序
            return year-y0;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(temp);
    }

    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        temp = dataInput.readInt();
    }
}

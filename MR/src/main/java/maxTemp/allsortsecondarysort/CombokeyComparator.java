package maxTemp.allsortsecondarysort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 排序对比器
 * @author LaZY（李志一）
 * @data 2019/1/8 - 20:24
 */
public class CombokeyComparator extends WritableComparator{


    public CombokeyComparator() {
        super(ComboKey.class,true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        ComboKey k1 = (ComboKey) a;
        ComboKey k2 = (ComboKey) b;
        return k1.getYear() - k2.getYear();
    }
}

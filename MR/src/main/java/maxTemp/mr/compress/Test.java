package maxTemp.mr.compress;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * @author LaZY（李志一）
 * @data 2019/1/7 - 22:16
 */
public class Test {
    public static void makeData() throws IOException {
        FileWriter fw = new FileWriter("h:/mr/maxtemp/temp.txt");
        for (int i = 0; i < 6000; i++) {
            int year = 1970 + new Random().nextInt(100);
            int temp = -30 + new Random().nextInt(100);
            fw.write(""+year+" "+temp+"\r\n");
        }
        fw.close();
    }
    public static void main(String[] args) {
        try {
            makeData();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

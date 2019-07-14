import java.util.Scanner;

/**
 * @author LaZY(李志一)
 * @create 2019-07-14 14:50
 */
public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String count = sc.nextLine();
        String string = sc.nextLine();
        if(string == null || string.length() < 1 || string.length()>5000){
            return;
        }
        if(string.length() <= 2){
            System.out.println(string);
            return;
        }
        int cou;
        try {
            cou = Integer.parseInt(count);
        }catch (Exception e){
            return;
        }
        String[] split = string.split(" ");
        if(split.length != cou){
            return;
        }
        StringBuffer s = new StringBuffer();

        for (int i = 0; i < split.length; i++) {
            int length = split[i].length();
            if (length <= 2){
                s = s.append(split[i]);
                if(i != split.length - 1){
                    s = s.append(" ");
                }
                continue;
            }
            StringBuffer s1 = fun(split[i].substring(0,length/2 ),split[i].substring(length/2 ,length));
            s = s.append(s1);
            if(i != split.length - 1){
                s = s.append(" ");
            }
        }
        System.out.println(s);
    }

    private static StringBuffer fun(String s1,String s2) {
        StringBuffer s = new StringBuffer();
        for (int i = 0,j = s2.length() - 1; i < s2.length()||j >=0; i++,j--) {
            if(!(i == s2.length()-1 && s2.length() > s1.length())){
                char c = s1.charAt(i);
                s= s.append(c);
            }
            char c1 = s2.charAt(j);
            s = s.append(c1);

        }
        return s;
    }

}

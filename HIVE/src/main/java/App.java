import java.sql.*;

/**
 * 使用jdbc方式连接数据仓库，数据仓库需要开启hiveserver2服务
 * @author LaZY（李志一）
 * @data 2019/1/13 - 23:11
 */
public class App {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.231.201:10000/mydb2");
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select id , name ,age from test2");
        while(rs.next()){
            System.out.println(rs.getInt(1) + "," + rs.getString(2)) ;
        }
        rs.close();
        st.close();
        conn.close();
    }
}

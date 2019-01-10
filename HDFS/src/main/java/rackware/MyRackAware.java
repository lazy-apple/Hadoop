package rackware;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.ArrayList;
import java.util.List;

/**
 * @author LaZY（李志一）
 * @data 2019/1/10 - 16:32
 */
public class MyRackAware implements DNSToSwitchMapping{
    public List<String> resolve(List<String> name) {
        List<String> list = new ArrayList<String>();
        //输出原来的信息，ip地址/主机名
        for (String s : name) {
            System.out.println(s);

            if(s.startsWith("192")){
                //192.168.231.202
                String ip = s.substring(s.lastIndexOf(".")+1);//提取子串
                if(Integer.parseInt(ip)<=203){
                    list.add("/rack1/"+ip);
                }else {
                    list.add("/rack2/"+ip);
                }

            }else if(s.startsWith("s")){
                String ip = s.substring(1);//提取子串
                if(Integer.parseInt(ip)<=203){
                    list.add("/rack1/"+ip);
                }else {
                    list.add("/rack2/"+ip);
                }
            }
        }

        return list;
    }

    public void reloadCachedMappings() {

    }

    public void reloadCachedMappings(List<String> list) {

    }
}

import etl.utils.IPSeekerEx;

import java.util.List;

public class TestIPEx {
    public static void main(String[] args) {
        IPSeekerEx ipSeekerEx = new IPSeekerEx();
        IPSeekerEx.RegionInfo info = ipSeekerEx.analyticIp("114.114.114.114");
        System.out.println(info);

        List<String> ips = ipSeekerEx.getAllIp();
        for (String ip : ips) {
            System.out.println(ipSeekerEx.analyticIp(ip));
        }
    }
}
import scala.Int;

import java.util.HashMap;
import java.util.Map;

public class test {


    public static void main(String[] args) {
        HashMap<String, Map<Integer, Integer>> map1 = new HashMap<>();

        Map<Integer, Integer> map2 = map1.get("1");
        String date = "1";
        if (map2==null){
            map2 = new HashMap<Integer, Integer>();
            map1.put(date,map2);
        }

        map2.put(1,1);
        map2.put(2,3);

        System.out.println(map1);
    }
}

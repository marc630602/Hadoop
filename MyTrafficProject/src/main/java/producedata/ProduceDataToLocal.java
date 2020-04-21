package producedata;

import common.utils.DateUtils;
import common.utils.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.*;
import java.util.*;

public class ProduceDataToLocal {
    public static String MONITOR_FLOW_ACTION ="./monitor_flow_action";
    public static String MONITOR_CAMERA_INFO ="./monitor_camera_info";

    public static void main(String[] args) {
        CreateFile(MONITOR_FLOW_ACTION);
        CreateFile(MONITOR_CAMERA_INFO);
        try{
            mock();
        } catch (IOException e ){
            e.printStackTrace();
        }
    }

    private static void mock() throws IOException {
        /**
         * 创建流对象
         */
        FileOutputStream fos = null ;
        OutputStreamWriter osw = null;
        PrintWriter pw = null ;

        File file = new File(MONITOR_FLOW_ACTION);//monitor_flow_action
        fos=new FileOutputStream(file,true);
        osw=new OutputStreamWriter(fos, "UTF-8");
        pw =new PrintWriter(osw);

        Random random = new Random();
        ArrayList<Row> dataList = new ArrayList<Row>();

        String[] locations = {"鲁", "京", "京", "京", "沪", "京", "京", "深", "京", "京"};
        String[] areas = new String[]{"海淀区","朝阳区","昌平区","东城区","西城区","丰台区","顺义区","大兴区"};
        String date = DateUtils.getTodayDate();

        /**
         * 模拟数据  数据格式如下：
         *
         *  日期   卡口ID    摄像头编号  车牌号	拍摄时间	 车速	 道路ID     区域ID
         * date	 monitor_id	 camera_id	 car   action_time	speed	 road_id   area_id
         *
         * monitor_flow_action
         */
        for (int i = 0; i < random.nextInt(3500)+2500 ; i++) {
            //模拟车牌号：如：京A00001
            String car = locations[random.nextInt(10)] + (char)(65+random.nextInt(26)) + StringUtils.fulfuill(5,random.nextInt(100000)+"");

            //baseActionTime 模拟24小时 2018-01-01 08
            String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24)+"");

            for (int j = 0; j < (random.nextInt(150)+50); j++) {
                if (j % 30 == 0 && j != 0 ){
                    baseActionTime = date + " " + StringUtils.fulfuill((Integer.parseInt(baseActionTime.split(" ")[1])+1)+"");
                }
                String areaId = StringUtils.fulfuill(2,random.nextInt(8)+1+"");//模拟areaId 【一共8个区域】

                String roadId = random.nextInt(50)+1+"";//模拟道路id 【1~50 个道路】

                String monitorId = StringUtils.fulfuill(4, random.nextInt(9)+"");//模拟9个卡扣monitorId，0补全4位

                String cameraId = StringUtils.fulfuill(5, random.nextInt(100000)+"");//模拟摄像头id cameraId

                String actionTime = baseActionTime + ":"
                        + StringUtils.fulfuill(random.nextInt(60)+"") + ":"
                        + StringUtils.fulfuill(random.nextInt(60)+"");//模拟经过此卡扣开始时间 ，如：2018-01-01 20:09:10

                String speed = (random.nextInt(260)+1)+"";//模拟速度

                //将数据写入到文件中
                String content = date+"\t"+monitorId+"\t"+cameraId+"\t"+car+"\t"+actionTime+"\t"+speed+"\t"+roadId+"\t"+areaId;
                pw.write(content+"\n");

                Row row = RowFactory.create(date,monitorId,cameraId,car,actionTime,speed,roadId,areaId);
                dataList.add(row);
            }
        }

        /**
         * monitorAndCameras    key：monitor_id
         * 						value:hashSet(camera_id)
         * 基于生成的数据，生成对应的卡扣号和摄像头对应基本表
         */
        HashMap<String, Set<String>> monitorAndCamare = new HashMap<>();
        int count = 0;
        for (Row row : dataList) {
            Set<String> sets = monitorAndCamare.get(row.getString(1));
            if (sets == null){
                sets = new HashSet<>();
                monitorAndCamare.put(row.getString(1),sets);
            }
            count++;
            if (count % 100 == 0 ){
                sets.add(StringUtils.fulfuill(5,random.nextInt(10000)+""));
            }
            sets.add(row.getString(2));
        }
        dataList.clear();

        /**
         * 创建对象
         */
        File file2 = new File(MONITOR_CAMERA_INFO);
        fos=new FileOutputStream(file2,true);
        osw=new OutputStreamWriter(fos, "UTF-8");
        pw =new PrintWriter(osw);

        Set<Map.Entry<String, Set<String>>> entrySet = monitorAndCamare.entrySet();
        for (Map.Entry<String, Set<String>> entry : entrySet) {
            String monitorId = entry.getKey();
            Set<String> camareIdSets = entry.getValue();
            Row row = null;
            for (String camareId : camareIdSets) {
                //将数据写入到文件
                String content = monitorId+"\t"+camareId;
                //产生一行模拟数据
                pw.write(content+"\n");
            }
        }
        pw.flush();
        pw.close();
        osw.close();
        fos.close();

    }

    private static Boolean CreateFile(String fileName) {
        try {
            File file = new File(fileName);
            if (file.exists()){
                file.delete();
            }
            boolean newFile = file.createNewFile();
            System.out.println("create file " + fileName + " sucess!" );
            return newFile;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}

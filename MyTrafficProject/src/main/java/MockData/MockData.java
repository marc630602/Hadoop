package MockData;



import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.*;

import common.utils.StringUtils;
import common.utils.DateUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class MockData {
    public static void mock(JavaSparkContext sc, SparkSession spark){
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

                Row row = RowFactory.create(date,monitorId,cameraId,car,actionTime,speed,roadId,areaId);
                dataList.add(row);
            }
        }

        JavaRDD<Row> rowRdd = sc.parallelize(dataList);

        StructType cameraFlowSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
                DataTypes.createStructField("camera_id", DataTypes.StringType, true),
                DataTypes.createStructField("car", DataTypes.StringType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("speed", DataTypes.StringType, true),
                DataTypes.createStructField("road_id", DataTypes.StringType, true),
                DataTypes.createStructField("area_id", DataTypes.StringType, true)
        ));

        Dataset<Row> ds = spark.createDataFrame(rowRdd, cameraFlowSchema);
        System.out.println("----打印 车辆信息数据----");
        ds.show();
        ds.registerTempTable("monitor_flow_action");


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

        Set<Map.Entry<String, Set<String>>> entrySet = monitorAndCamare.entrySet();
        for (Map.Entry<String, Set<String>> entry : entrySet) {
            String monitorId = entry.getKey();
            Set<String> camareIdSets = entry.getValue();
            Row row = null;
            for (String camareId : camareIdSets) {
                row = RowFactory.create(monitorId,camareId);
                dataList.add(row);
            }
        }

        StructType monitorSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
                DataTypes.createStructField("camera_id", DataTypes.StringType, true)
        ));


        rowRdd = sc.parallelize(dataList);
        Dataset<Row> monitorDF = spark.createDataFrame(rowRdd, monitorSchema);
        monitorDF.registerTempTable("monitor_camera_info");
        System.out.println("----打印 卡扣号对应摄像头号 数据----");
        monitorDF.show();
    }
}

package common.client;

import MockData.MockData;
import com.alibaba.fastjson.JSONObject;
import com.google.inject.internal.cglib.core.$Constants;
import common.conf.ConfigurationManager;
import common.constant.Constants;
import common.dao.ITaskDAO;
import common.dao.factory.DAOFactory;
import common.domain.Task;
import common.utils.DateUtils;
import common.utils.NumberUtils;
import common.utils.ParamUtils;
import common.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

/**
 * 9.计算卡扣流量转换率
 * 	一辆车的轨迹：
 * 		0001->0002->0001->0003->0001->0002
 * 	    卡扣0001到卡扣0002的车流量转化率：2/3
 */
public class MonitorConvertRateAnalyze {
    public static void main(String[] args) {
        /**
         * 判断应用程序是否在本地执行
         */
        JavaSparkContext sc = null;
        SparkSession spark = null;
        Boolean onLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if(onLocal){
            // 构建Spark运行时的环境参数
            SparkConf conf = new SparkConf()
                    .setAppName(Constants.SPARK_APP_NAME)
//			.set("spark.sql.shuffle.partitions", "300")
//			.set("spark.default.parallelism", "100")
//			.set("spark.storage.memoryFraction", "0.5")
//			.set("spark.shuffle.consolidateFiles", "true")
//			.set("spark.shuffle.file.buffer", "64")
//			.set("spark.shuffle.memoryFraction", "0.3")
//			.set("spark.reducer.maxSizeInFlight", "96")
//			.set("spark.shuffle.io.maxRetries", "60")
//			.set("spark.shuffle.io.retryWait", "60")
//			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//			.registerKryoClasses(new Class[]{SpeedSortKey.class})
                    ;
            /**
             * 设置spark运行时的master  根据配置文件来决定的
             */
            conf.setMaster("local");
            sc = new JavaSparkContext(conf);

            spark = SparkSession.builder().getOrCreate();
            /**
             * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的表就可以
             * 本地模拟数据注册成一张临时表
             * monitor_flow_action	数据表：监控车流量所有数据
             * monitor_camera_info	标准表：卡扣对应摄像头标准表
             */
            MockData.mock(sc, spark);
        }else{
            System.out.println("++++++++++++++++++++++++++++++++++++++开启hive的支持");
            /**
             * "SELECT * FROM table1 join table2 ON (连接条件)"  如果某一个表小于20G 他会自动广播出去
             * 会将小于spark.sql.autoBroadcastJoinThreshold值（默认为10M）的表广播到executor节点，不走shuffle过程,更加高效。
             *
             * config("spark.sql.autoBroadcastJoinThreshold", "1048576000");  //单位：字节
             */
            spark = SparkSession.builder().config("spark.sql.autoBroadcastJoinThreshold", "1048576000").enableHiveSupport().getOrCreate();
            spark.sql("use traffic");
            sc = new JavaSparkContext(spark.sparkContext());
            }

            Long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_MONITOR_ONE_STEP_CONVERT);
            if(taskId == 0L) {
                System.out.println("args is null.....");
                return;
            }
            /**
             * 获取ITaskDAO的对象，通过taskId查询出来的数据封装到Task（自定义）对象
             */
                ITaskDAO taskDAO = DAOFactory.getTaskDAO();
                Task task = taskDAO.findTaskById(taskId);

            if(task == null){
                return;
            }

            /**
             * task.getTaskParams()是一个json格式的字符串   封装到taskParamsJsonObject
             * 将 task_parm字符串转换成json格式数据。
             */
            JSONObject taskParams = JSONObject.parseObject(task.getTaskParams());

            String roadFlow = ParamUtils.getParam(taskParams, Constants.PARAM_MONITOR_FLOW);
            Broadcast<String> roadFlowbc = sc.broadcast(roadFlow);

            JavaRDD<Row> rowRDDByDateRange = SparkUtils.getCameraRDDByDateRange(spark, taskParams);

            JavaPairRDD<String, Long> roadSplitRDD = generateAndMatchRowSplit(taskParams, roadFlowbc, rowRDDByDateRange);

            Map<String, Long> roadFlow2Count = getRoadFlowCount(roadSplitRDD);

            Map<String, Double> convertRateMap = computeRoadSplitConvertRate(roadFlow,roadFlow2Count);

            for (Map.Entry<String, Double> entry : convertRateMap.entrySet()) {
                System.out.println(entry.getKey()+"="+entry.getValue());
            }
        }


    private static Map<String, Double> computeRoadSplitConvertRate(String roadFlow, Map<String, Long> roadFlow2Count) {
        //[0001，0002，0003，0004，0005]
        String[] splits = roadFlow.split(",");

        /**
         * 存放卡扣切面的转换率
         * "0001,0002" 0.16
         */
        Map<String, Double> rateMap = new HashMap<>();
        long lastMonitorCarCount = 0L;
        String tmpRoadFlow = "";
        for (int i = 0; i < splits.length; i++) {
            tmpRoadFlow += "," + splits[i];
            Long count = roadFlow2Count.get(tmpRoadFlow.substring(1));
            if (count !=0L && i != 0 && lastMonitorCarCount != 0L){
                double rate = NumberUtils.formatDouble((double)count/(double)lastMonitorCarCount,2);
                rateMap.put(tmpRoadFlow.substring(1), rate);
            }
            lastMonitorCarCount = count;
        }
        return rateMap;
    }

    private static Map<String, Long> getRoadFlowCount(JavaPairRDD<String, Long> roadSplitRDD) {
        JavaPairRDD<String, Long> sumBtKey = roadSplitRDD.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        Map<String, Long> map = sumBtKey.collectAsMap();
        return map;
    }

    private static JavaPairRDD<String, Long> generateAndMatchRowSplit(JSONObject taskParams,
                                                 Broadcast<String> roadFlowbc,
                                                 JavaRDD<Row> rowRDDByDateRange) {
        JavaPairRDD<String, Long> roadSplitRDD = rowRDDByDateRange.mapToPair(new PairFunction<Row, String, Row>() {

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getAs("car") + "", row);
            }
        }).groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Long>() {

            @Override
            public Iterator<Tuple2<String, Long>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String car = tuple._1;
                Iterator<Row> row = tuple._2.iterator();
                //初始化返回结果
                List<Tuple2<String, Long>> list = new ArrayList<>();
                //初始化存储row信息的列表
                List<Row> rows = new ArrayList<>();
                while (row.hasNext()) {
                    rows.add(row.next());
                }

                Collections.sort(rows, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String action_time1 = o1.getAs("action_time").toString();
                        String action_time2 = o2.getAs("action_time").toString();
                        return DateUtils.before(action_time1, action_time2) ? -1 : 1;
                    }
                });

                //初始化车辆轨迹
                StringBuilder carTrackBuilder = new StringBuilder();

                for (Row row1 : rows) {
                    carTrackBuilder.append("," + row1.getString(1));
                }

                /**
                 * 获取指定卡扣流
                 */
                String carTrack = carTrackBuilder.substring(1);
                String roadFlow = roadFlowbc.value();
                /**
                 * 对指定的卡扣流参数分割
                 */
                String[] splits = roadFlow.split(",");

                for (int i = 0; i < splits.length; i++) {
                    String tmpRoadFlow = "";
                    for (int j = 0; j < i; j++) {
                        tmpRoadFlow += "," + splits[i];
                    }
                    tmpRoadFlow = tmpRoadFlow.substring(1);

                    //indexOf 从哪个位置开始查找
                    int index = 0;
                    //这辆车有多少次匹配到这个卡扣切片的次数
                    Long count = 0L;
                    while (carTrack.indexOf(tmpRoadFlow, index) != -1) {
                        index = carTrack.indexOf(tmpRoadFlow, index) + 1;
                        count++;
                    }
                    list.add(new Tuple2<>(tmpRoadFlow, count));
                }

                return list.iterator();
            }
        });
        return roadSplitRDD;
    }
}

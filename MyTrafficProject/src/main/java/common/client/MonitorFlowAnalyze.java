package common.client;


import MockData.MockData;
import com.alibaba.fastjson.JSONObject;
import common.conf.ConfigurationManager;
import common.constant.Constants;
import common.dao.IMonitorDAO;
import common.dao.ITaskDAO;
import common.dao.factory.DAOFactory;
import common.domain.*;
import common.utils.DateUtils;
import common.utils.ParamUtils;
import common.utils.SparkUtils;
import common.utils.StringUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import java.util.*;

/**
 * 1.卡扣监控
 *      正常的卡扣数
 * 	    异常的卡扣数
 * 	    正常的摄像头个数
 * 	    异常的摄像头个数
 * 	    异常的摄像头详细信息
 *
 * 2.车流量top5的卡扣
 *
 * 3.top5卡扣下所有车辆详细信息
 *
 * 4.获取车辆高速通过的TOPN卡扣
 *
 * 5.车辆高速这5个卡扣每个卡扣下车辆速度top10
 *
 * 6.碰撞分析 : 01,02中这一天同时出现的车辆
 *
 * 7.车辆轨迹
 *
 * @param <onLocal>
 */
public class MonitorFlowAnalyze<onLocal> {
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
            //这里不会真正的创建SparkSession,而是根据前面这个SparkContext来获取封装SparkSession,因为不会创建存在两个SparkContext的。
            spark = SparkSession.builder().getOrCreate();
            /**
             * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的表就可以
             * 本地模拟数据注册成一张临时表
             * monitor_flow_action	数据表：监控车流量所有数据
             * monitor_camera_info	标准表：卡扣对应摄像头标准表
             */
            MockData.mock(sc,spark);
        } else {
            System.out.println("++++++++++++++++++++++++++++++++++++++开启hive的支持");
            /**
             * "SELECT * FROM table1 join table2 ON (连接条件)"  如果某一个表小于20G 他会自动广播出去
             * 会将小于spark.sql.autoBroadcastJoinThreshold值（默认为10M）的表广播到executor节点，不走shuffle过程,更加高效。
             *
             * config("spark.sql.autoBroadcastJoinThreshold", "1048576000");  //单位：字节
             */
            spark = SparkSession.builder().appName(Constants.SPARK_APP_NAME).enableHiveSupport().getOrCreate();
            sc = new JavaSparkContext(spark.sparkContext());
            spark.sql("use traffic");
        }

        /**
         * 从配置文件my.properties中拿到spark.local.taskId.monitorFlow的taskId
         */
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_MONITOR);
        if(taskId == 0L){
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

        /**
         * 通过params（json字符串）查询monitor_flow_action
         *
         * 获取指定日期内检测的monitor_flow_action中车流量数据，返回JavaRDD<Row>
         */
        JavaRDD<Row> cameraRDD = SparkUtils.getCameraRDDByDateRange(spark, taskParams);

        /**
         * 创建了一个自定义的累加器
         */
        SelfDefineAccumulator monitorAndCameraStateAccumulator = new  SelfDefineAccumulator();
        spark.sparkContext().register(monitorAndCameraStateAccumulator,"SelfAccumulator");

        /**
         * 将row类型的RDD 转换成kv格式的RDD   k:monitor_id  v:row
         */
        JavaPairRDD<String, Row> monitor2DetailRDD = getMonitor2DetailRDD(cameraRDD);
        /**
         * monitor2DetailRDD进行了持久化
         */
        monitor2DetailRDD = monitor2DetailRDD.cache();
        /**
         * 按照卡扣号分组，对应的数据是：每个卡扣号(monitor)对应的Row信息
         * 由于一共有9个卡扣号，这里groupByKey后一共有9组数据。
         */
        JavaPairRDD<String, Iterable<Row>> monitorId2RowsRDD = monitor2DetailRDD.groupByKey();


        /**
         * 遍历分组后的RDD，拼接字符串
         * 数据中一共就有9个monitorId信息，那么聚合之后的信息也是9条
         * monitor_id=|cameraIds=|area_id=|camera_count=|carCount=
         * 例如:
         * ("0005","monitorId=0005|camearIds=09200,03243,02435,03232|cameraCount=4|carCount=100")
         *
         */
        JavaPairRDD<String, String> aggregateMonitorId2DetailRDD = aggreagteByMonitor(monitorId2RowsRDD);

        /**
         * 检测卡扣状态
         * carCount2MonitorRDD
         * K:car_count V:monitor_id
         * RDD(卡扣对应车流量总数,对应的卡扣号)
         */
        JavaPairRDD<Integer, String> carCount2MonitorRDD =
                checkMonitorState(sc,spark,aggregateMonitorId2DetailRDD,taskId,taskParams,monitorAndCameraStateAccumulator);
        /**
         * action 类算子触发以上操作
         *
         */
        carCount2MonitorRDD.count();

        /**
         * 往数据库表  monitor_state 中保存 累加器累加的五个状态
         */
        saveMonitorState(taskId,monitorAndCameraStateAccumulator);

        /**
         * 获取车流排名前N的卡扣号
         * 并放入数据库表  topn_monitor_car_count 中
         * return  KV格式的RDD  K：monitor_id V:monitor_id
         * 返回的是topN的(monitor_id,monitor_id)
         */
        JavaPairRDD<String, String> topNMonitor2CarFlow =
                getTopNMonitorCarFlow(sc,taskId,taskParams,carCount2MonitorRDD);

        /**
         * 获取topN 卡口的车流量具体信息，存入数据库表 topn_monitor_detail_info 中
         */
        getTopNDetails(taskId,topNMonitor2CarFlow,monitor2DetailRDD);

        /**
         * 获取车辆高速通过的TOPN卡扣
         */
        List<String> top5MonitorIds = speedTopNMonitor(monitorId2RowsRDD);
        for (String monitorId : top5MonitorIds) {
            System.out.println("车辆经常高速通过的卡扣	monitorId:"+monitorId);
        }

        /**
         * 获取车辆高速通过的TOPN卡扣，每一个卡扣中车辆速度最快的前10名，并存入数据库表 top10_speed_detail 中
         */
        getMonitorDetails(sc,taskId,top5MonitorIds,monitor2DetailRDD);

        /**
         * 区域碰撞分析,直接打印显示出来。
         * "01","02" 指的是两个区域
         */
        CarPeng(spark,taskParams,"01","02");

        /************车辆轨迹**cameraRDD************/
        //从所有数据中找出卡扣 0001 下的车辆
        List<String> cars = cameraRDD.filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return "0001".equals(row.getAs("monitor_id").toString());
            }
        }).map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getAs("car") + "";
            }
        }).distinct().take(20);

        Broadcast<List<String>> bcCars = sc.broadcast(cars);

        cameraRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getAs("car")+"",row);
            }
        }).filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                return bcCars.value().contains(tuple._1);
            }
        }).groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Row>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String car = tuple._1;
                List<Row> rowList = IteratorUtils.toList(tuple._2.iterator());
                Collections.sort(rowList, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String t1 = o1.getAs("action_time")+"";
                        String t2 = o2.getAs("action_time")+"";
                        return DateUtils.after(t1,t2) ? 1 : -1;
                    }
                });
                String track = "";
                for (Row row : rowList) {
                    track += row.getAs("monitor_id")+" at "+row.getAs("action_time") + "-->";
                }
                System.out.println("car: " + car + " Track of today " + track);
            }
        });

        System.out.println("******All is finished*******");
        sc.close();
    }

    /**
     * 区域碰撞分析：两个区域共同出现的车辆
     * @param area1
     * @param area2
     */
    private static void CarPeng(SparkSession spark,JSONObject taskParamsJsonObject,String area1,String area2){
        //得到01区域的数据放入rdd01
        JavaRDD<Row> cameraRDD01 = SparkUtils.getCameraRDDByDateRangeAndArea(spark, taskParamsJsonObject,area1);

        //得到02区域的数据放入rdd02
        JavaRDD<Row> cameraRDD02 = SparkUtils.getCameraRDDByDateRangeAndArea(spark, taskParamsJsonObject,area2);

        JavaRDD<String> distinct1 = cameraRDD01.map(new Function<Row, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(Row row)  {
                return row.getAs("car");
            }
        }).distinct();

        JavaRDD<String> distinct2 = cameraRDD02.map(new Function<Row, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(Row row)  {
                return row.getAs("car");
            }
        }).distinct();

        distinct1.intersection(distinct2).foreach(new VoidFunction<String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(String car)  {
                System.out.println("01 ，02 区域同时出现的car ***** "+car);
            }
        });
    }

    private static void getMonitorDetails(JavaSparkContext sc, long taskId, List<String> top5MonitorIds, JavaPairRDD<String, Row> monitor2DetailRDD) {
        /**
         * top5MonitorIds这个集合里面都是monitor_id
         */
        final Broadcast<List<String>> top5MonitorIdsBroadcast = sc.broadcast(top5MonitorIds);

        /**
         * 我们想获取每一个卡扣的详细信息，就是从monitor2DetailRDD中取出来包含在top10MonitorIds集合的卡扣的信息
         */
        monitor2DetailRDD.filter(new Function<Tuple2<String,Row>, Boolean>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<String, Row> tuple)  {
                return top5MonitorIdsBroadcast.value().contains(tuple._1);
            }
        }).groupByKey().foreach(new VoidFunction<Tuple2<String,Iterable<Row>>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Iterable<Row>> tuple)  {
                String monitor_id = tuple._1;

                Iterator<Row> rowsIterator = tuple._2.iterator();

                Row[] top10Cars = new Row[10];
                while (rowsIterator.hasNext()) {
                    Row row = rowsIterator.next();

                    long speed = Long.parseLong(row.getString(5));

                    for(int i = 0; i < top10Cars.length; i++) {
                        if(top10Cars[i] == null) {
                            top10Cars[i] = row;
                            break;
                        } else {
                            long _speed = Long.parseLong(top10Cars[i].getString(5));
                            if(speed > _speed) {
                                for(int j = 9; j > i; j--) {
                                    top10Cars[j] = top10Cars[j - 1];
                                }
                                top10Cars[i] = row;
                                break;
                            }
                        }
                    }
                }

                /**
                 * 将车辆通过速度最快的前N个卡扣中每个卡扣通过的车辆的速度最快的前10名存入数据库表 top10_speed_detail中
                 */
                List<TopNMonitorDetailInfo> topNMonitorDetailInfos = new ArrayList<>();
                for (Row row : top10Cars) {
                    topNMonitorDetailInfos.add(new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6)));
                }
                IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                monitorDAO.insertBatchTop10Details(topNMonitorDetailInfos);
            }
        });
    }

    /**
     * 获取经常高速通过的TOPN卡扣 , 返回车辆经常高速通过的卡扣List
     *
     * 1、每一辆车都有speed    按照速度划分是否是高速 中速 普通 低速
     * 2、每一辆车的车速都在一个车速段     对每一个卡扣进行聚合   拿到高速通过 中速通过  普通  低速通过的车辆各是多少辆
     * 3、四次排序   先按照高速通过车辆数   中速通过车辆数   普通通过车辆数   低速通过车辆数
     * @param groupByMonitorId ---- (monitorId ,Iterable[Row])
     * @return List<MonitorId> 返回车辆经常高速通过的卡扣List
     */
    private static List<String> speedTopNMonitor(JavaPairRDD<String, Iterable<Row>> groupByMonitorId) {
        /**
         * key:自定义的类  value：卡扣ID
         */
        JavaPairRDD<SpeedSortKey, String> speedSortKey2MonitorId =
                groupByMonitorId.mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, SpeedSortKey,String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<SpeedSortKey, String> call(Tuple2<String, Iterable<Row>> tuple)  {
                        String monitorId = tuple._1;
                        Iterator<Row> speedIterator = tuple._2.iterator();

                        /**
                         * 这四个遍历 来统计这个卡扣下 高速 中速 正常 以及低速通过的车辆数
                         */
                        long lowSpeed = 0;
                        long normalSpeed = 0;
                        long mediumSpeed = 0;
                        long highSpeed = 0;

                        while(speedIterator.hasNext()){
                            int speed = StringUtils.convertStringtoInt(speedIterator.next().getString(5));
                            if(speed >= 0 && speed < 60){
                                lowSpeed ++;
                            }else if (speed >= 60 && speed < 90) {
                                normalSpeed ++;
                            }else if (speed >= 90 && speed < 120) {
                                mediumSpeed ++;
                            }else if (speed >= 120) {
                                highSpeed ++;
                            }
                        }
                        SpeedSortKey speedSortKey = new SpeedSortKey(lowSpeed,normalSpeed,mediumSpeed,highSpeed);
                        return new Tuple2<>(speedSortKey, monitorId);
                    }
                });
        /**
         * key:自定义的类  value：卡扣ID
         */
        JavaPairRDD<SpeedSortKey, String> sortBySpeedCount = speedSortKey2MonitorId.sortByKey(false);
        /**
         * 硬编码问题
         * 取出前5个经常速度高的卡扣
         */
        List<Tuple2<SpeedSortKey, String>> take = sortBySpeedCount.take(5);

        List<String> monitorIds = new ArrayList<>();
        for (Tuple2<SpeedSortKey, String> tuple : take) {
            monitorIds.add(tuple._2);
            System.out.println("monitor_id = "+tuple._2+"-----"+tuple._1);
        }
        return monitorIds;
    }



    private static void getTopNDetails(long taskId,
                                       JavaPairRDD<String, String> topNMonitor2CarFlow,
                                       JavaPairRDD<String, Row> monitor2DetailRDD) {
//        /*******************************************
//         * 使用广播变量,之后再foreach
//         *******************************************/
//        List<String> top5MonitorId = topNMonitor2CarFlow.map(new Function<Tuple2<String, String>, String>() {
//            @Override
//            public String call(Tuple2<String, String> tp) throws Exception {
//                return tp._1;
//            }
//        }).collect();
//        //获取sc,然后获取广播变量
//        JavaSparkContext sc = new JavaSparkContext(monitor2DetailRDD.context());
//        Broadcast<List<String>> broadcast = sc.broadcast(top5MonitorId);
//
//        JavaPairRDD<String, Row> filterTopNMonitor2CarFlow = monitor2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
//            @Override
//            public Boolean call(Tuple2<String, Row> tp) throws Exception {
//                return broadcast.value().contains(tp._1);
//            }
//        });

        /**
         * 获取车流量排名前N的卡口的详细信息   可以看一下是在什么时间段内卡口流量暴增的。 (使用join)
         */
        topNMonitor2CarFlow.join(monitor2DetailRDD).mapToPair(
                new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> t)  {
                        return new Tuple2<String, Row>(t._1, t._2._2);
                    }
                }).foreachPartition(new VoidFunction<Iterator<Tuple2<String,Row>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Tuple2<String, Row>> t)  {

                List<TopNMonitorDetailInfo> monitorDetailInfos = new ArrayList<>();

                while (t.hasNext()) {
                    Tuple2<String, Row> tuple = t.next();
                    Row row = tuple._2;
                    TopNMonitorDetailInfo m = new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6));
                    monitorDetailInfos.add(m);
                }
                /**
                 * 将topN的卡扣车流量明细数据 存入topn_monitor_detail_info 表中
                 */
                IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                monitorDAO.insertBatchMonitorDetails(monitorDetailInfos);
            }
        });
    }



    private static JavaPairRDD<String, String> getTopNMonitorCarFlow(JavaSparkContext sc,
                                                                     long taskId,
                                                                     JSONObject taskParamsJsonObject,
                                                                     JavaPairRDD<Integer, String> carCount2MonitorId) {
        int topNumFromParams = Integer.parseInt(ParamUtils.getParam(taskParamsJsonObject,Constants.FIELD_TOP_NUM));

        /**
         * carCount2MonitorId <carCount,monitor_id>
         */
        List<Tuple2<Integer, String>> topNCarCount = carCount2MonitorId.sortByKey(false).take(topNumFromParams);

        //封装到对象中
        List<TopNMonitor2CarCount> topNMonitor2CarCounts = new ArrayList<>();
        for (Tuple2<Integer, String> tuple : topNCarCount) {
            TopNMonitor2CarCount topNMonitor2CarCount = new TopNMonitor2CarCount(taskId,tuple._2,tuple._1);
            topNMonitor2CarCounts.add(topNMonitor2CarCount);
        }

        /**
         * 得到DAO 将数据插入数据库
         * 向数据库表 topn_monitor_car_count 中插入车流量最多的TopN数据
         */
        IMonitorDAO ITopNMonitor2CarCountDAO = DAOFactory.getMonitorDAO();
        ITopNMonitor2CarCountDAO.insertBatchTopN(topNMonitor2CarCounts);

        /**
         * monitorId2MonitorIdRDD ---- K:monitor_id V:monitor_id
         * 获取topN卡口的详细信息
         * monitorId2MonitorIdRDD.join(monitorId2RowRDD)
         */
        List<Tuple2<String, String>> monitorId2CarCounts = new ArrayList<>();
        for(Tuple2<Integer,String> t : topNCarCount){
            monitorId2CarCounts.add(new Tuple2<String, String>(t._2, t._2));
        }
        JavaPairRDD<String, String> monitorId2MonitorIdRDD = sc.parallelizePairs(monitorId2CarCounts);
        return monitorId2MonitorIdRDD;
    }

    /**
     * 往数据库中保存 累加器累加的五个状态
     * @param taskId
     * @param monitorAndCameraStateAccumulator
     */
    private static void saveMonitorState(long taskId, SelfDefineAccumulator monitorAndCameraStateAccumulator) {
        /**
         * 累加器中值能在Executor段读取吗？
         * 		不能
         * 这里的读取时在Driver中进行的
         */
        String accumulatorVal = monitorAndCameraStateAccumulator.value();
        String normalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_MONITOR_COUNT);
        String normalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_CAMERA_COUNT);
        String abnormalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_COUNT);
        String abnormalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_CAMERA_COUNT);
        String abnormalMonitorCameraInfos = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS);

        /**
         * 这里面只有一条记录
         */
        MonitorState monitorState = new MonitorState(taskId, normalMonitorCount, normalCameraCount, abnormalMonitorCount, abnormalCameraCount, abnormalMonitorCameraInfos);

        /**
         * 向数据库表monitor_state中添加累加器累计的各个值
         */
        IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
        monitorDAO.insertMonitorState(monitorState);
    }

    /**
     * 检测卡口状态
     * @return RDD(实际卡扣对应车流量总数,对应的卡扣号)
     */
    private static JavaPairRDD<Integer, String> checkMonitorState(JavaSparkContext sc,
                                                                  SparkSession spark,
                                                                  JavaPairRDD<String, String> aggregateMonitorId2DetailRDD,
                                                                  final long taskId,
                                                                  JSONObject taskParamsJsonObject,
                                                                  SelfDefineAccumulator monitorAndCameraStateAccumulator) {

        /**
         * 从monitor_camera_info标准表中查询出来每一个卡口对应的camera的数量
         */
        String sqlText = "SELECT * FROM monitor_camera_info";
        Dataset<Row> standardDF = spark.sql(sqlText);
        JavaRDD<Row> standardRDD = standardDF.javaRDD();

        /**
         * 使用mapToPair算子将standardRDD变成KV格式的RDD
         * monitorId2CameraId   :
         * (K:monitor_id  v:camera_id)
         */
        JavaPairRDD<String, String> monitorId2CameraId = standardRDD.mapToPair(
                new PairFunction<Row, String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(Row row)  {
                        return new Tuple2<String, String>(row.getString(0), row.getString(1));
                    }
                });

        /**
         * 对每一个卡扣下面的信息进行统计，统计出来camera_count（这个卡扣下一共有多少个摄像头）,camera_ids(这个卡扣下，所有的摄像头编号拼接成的字符串)
         * 返回：
         * 	("monitorId","cameraIds=xxx|cameraCount=xxx")
         * 例如：
         * 	("0008","cameraIds=02322,01213,03442|cameraCount=3")
         * 如何来统计？
         * 	1、按照monitor_id分组
         * 	2、使用mapToPair遍历，遍历的过程可以统计
         */
        JavaPairRDD<String, String> standardMonitor2CameraInfos = monitorId2CameraId.groupByKey()
                .mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<String>> tuple)  {
                String monitorId = tuple._1;
                Iterator<String> cameraIterator = tuple._2.iterator();
                int count = 0;
                StringBuilder cameraIds = new StringBuilder();
                while(cameraIterator.hasNext()){
                    cameraIds.append(","+cameraIterator.next());
                    count++;
                }
                //cameraIds=00001,00002,00003,00004|cameraCount=4
                String cameraInfos = Constants.FIELD_CAMERA_IDS+"="+cameraIds.toString().substring(1)+"|"
                        +Constants.FIELD_CAMERA_COUNT+"="+count;
                return new Tuple2<String, String>(monitorId,cameraInfos);
            }
        });

        /**
         * 将两个RDD进行比较，join  leftOuterJoin
         * 为什么使用左外连接？ 左：标准表里面的信息  右：实际信息
         */
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joinResultRDD =
                standardMonitor2CameraInfos.leftOuterJoin(aggregateMonitorId2DetailRDD);

        /**
         * carCount2MonitorId 最终返回的K,V格式的数据
         * K：实际监测数据中某个卡扣对应的总车流量
         * V：实际监测数据中这个卡扣 monitorId
         */
        JavaPairRDD<Integer, String> carCount2MonitorId = joinResultRDD.mapPartitionsToPair(
                new PairFlatMapFunction<
                                        Iterator<Tuple2<String,Tuple2<String,Optional<String>>>>, Integer, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<Integer, String>> call(
                            Iterator<Tuple2<String, Tuple2<String, Optional<String>>>> iterator)  {

                        List<Tuple2<Integer, String>> list = new ArrayList<>();
                        while (iterator.hasNext()) {
                            //储藏返回值
                            Tuple2<String, Tuple2<String, Optional<String>>> tuple = iterator.next();
                            String monitorId = tuple._1;
                            String standardCameraInfos = tuple._2._1;
                            Optional<String> factCameraInfosOptional = tuple._2._2;
                            String factCameraInfos = "";

                            if(factCameraInfosOptional.isPresent()){
                                //这里面是实际检测数据中有标准卡扣信息
                                factCameraInfos = factCameraInfosOptional.get();
                            }else{
                                String standardCameraIds =
                                        StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);
                                String abnoramlCameraCount =
                                        StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT);

                                //abnormalMonitorCount=1|abnormalCameraCount=3|abnormalMonitorCameraInfos="0002":07553,07554,07556
                                monitorAndCameraStateAccumulator.add(
                                        Constants.FIELD_ABNORMAL_MONITOR_COUNT +"=1|"
                                                +Constants.FIELD_ABNORMAL_CAMERA_COUNT+"="+abnoramlCameraCount+"|"
                                                +Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS+"="+monitorId+":"+standardCameraIds);
                                //跳出了本次while
                                continue;
                            }
                            /**
                             * 从实际数据拼接的字符串中获取摄像头数
                             */
                            int factCameraCount = Integer.parseInt(StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT));
                            /**
                             * 从标准数据拼接的字符串中获取摄像头数
                             */
                            int standardCameraCount = Integer.parseInt(StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT));
                            if(factCameraCount == standardCameraCount){
                                /*
                                 * 	1、正常卡口数量
                                 * 	2、异常卡口数量
                                 * 	3、正常通道（此通道的摄像头运行正常）数，通道就是摄像头
                                 * 	4、异常卡口数量中哪些摄像头异常，需要保存摄像头的编号
                                 */
                                monitorAndCameraStateAccumulator.add(Constants.FIELD_NORMAL_MONITOR_COUNT+"=1|"+Constants.FIELD_NORMAL_CAMERA_COUNT+"="+factCameraCount);
                            }else{
                                /**
                                 * 从实际数据拼接的字符串中获取摄像编号集合
                                 */
                                String factCameraIds = StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);

                                /**
                                 * 从标准数据拼接的字符串中获取摄像头编号集合
                                 */
                                String standardCameraIds = StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);

                                List<String> factCameraIdList = Arrays.asList(factCameraIds.split(","));
                                List<String> standardCameraIdList = Arrays.asList(standardCameraIds.split(","));
                                StringBuilder abnormalCameraInfos = new StringBuilder();
                                int abnormalCameraCount = 0;//不正常摄像头数
                                int normalCameraCount = 0;//正常摄像头数
                                for (String cameraId : standardCameraIdList) {
                                    if(!factCameraIdList.contains(cameraId)){
                                        abnormalCameraCount++;
                                        abnormalCameraInfos.append(",").append(cameraId);
                                    }
                                }
                                normalCameraCount = standardCameraIdList.size()-abnormalCameraCount;
                                //往累加器中更新状态
                                monitorAndCameraStateAccumulator.add(
                                        Constants.FIELD_ABNORMAL_MONITOR_COUNT+"=1|"
                                                +Constants.FIELD_NORMAL_CAMERA_COUNT+"="+normalCameraCount+"|"
                                                +Constants.FIELD_ABNORMAL_CAMERA_COUNT+"="+abnormalCameraCount+"|"
                                                +Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS+"="+monitorId + ":" + abnormalCameraInfos.toString().substring(1));
                            }
                            //从实际数据拼接到字符串中获取车流量
                            int carCount = Integer.parseInt(StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAR_COUNT));
                            list.add(new Tuple2<Integer, String>(carCount,monitorId));
                        }
                        //最后返回的list是实际监测到的数据中，list[(卡扣对应车流量总数,对应的卡扣号),... ...]
                        return  list.iterator();
                    }
                });
        return carCount2MonitorId;
    }


    /**
     * 按照monitor_id进行聚和
     * @return ("monitorId","monitorId=xxx|areaId=xxx|cameraIds=xxx|cameraCount=xxx|carCount=xxx")
     * ("0005","monitorId=0005|areaId=02|camearIds=09200,03243,02435,03232|cameraCount=4|carCount=100")
     * 假设其中一条数据是以上这条数据，那么说明在这个0005卡扣下有4个camera,那么这个卡扣一共通过了100辆车信息.
     */
    private static JavaPairRDD<String, String> aggreagteByMonitor(JavaPairRDD<String, Iterable<Row>> monitorId2RowsRDD) {

        /**
         * 一个monitor_id对应一条记录
         * 为什么使用mapToPair来遍历数据，因为我们要操作的返回值是每一个monitorid 所对应的详细信息
         */
        JavaPairRDD<String, String> monitorId2CameraCountRDD = monitorId2RowsRDD.mapToPair(
                new PairFunction<Tuple2<String,Iterable<Row>>,String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple)  {
                        String monitorId = tuple._1;
                        Iterator<Row> rowIterator = tuple._2.iterator();

                        List<String> list = new ArrayList<>();//同一个monitorId下，对应的所有的不同的cameraId,list.count方便知道此monitor下对应多少个cameraId

                        StringBuilder tmpInfos = new StringBuilder();//同一个monitorId下，对应的所有的不同的camearId信息

                        int count = 0;//统计车辆数的count
                        /**
                         * 这个while循环  代表的是当前的这个卡扣一共经过了多少辆车，   一辆车的信息就是一个row
                         */
                        while(rowIterator.hasNext()){
                            Row row = rowIterator.next();
                            String cameraId = row.getString(2);
                            if(!list.contains(cameraId)){
                                list.add(cameraId);
                            }
                            //针对同一个卡扣 monitor，append不同的cameraId信息
                            if(!tmpInfos.toString().contains(cameraId)){
                                tmpInfos.append(","+cameraId);
                            }
                            //这里的count就代表的车辆数，一个row一辆车
                            count++;
                        }

                        /**
                         * camera_count
                         */
                        int cameraCount = list.size();
                        //monitorId=0001|cameraIds=00001,00002,00003|cameraCount=3|carCount=100
                        String infos =  Constants.FIELD_MONITOR_ID+"="+monitorId+"|"
                                +Constants.FIELD_CAMERA_IDS+"="+tmpInfos.toString().substring(1)+"|"
                                +Constants.FIELD_CAMERA_COUNT+"="+cameraCount+"|"
                                +Constants.FIELD_CAR_COUNT+"="+count;
                        return new Tuple2<String, String>(monitorId, infos);
                    }
                });
        //<monitor_id,camera_infos(ids,cameracount,carCount)>
        return monitorId2CameraCountRDD;
    }

    /**
     * 将RDD转换成K,V格式的RDD
     * @param cameraRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getMonitor2DetailRDD(JavaRDD<Row> cameraRDD) {
        JavaPairRDD<String, Row> monitorId2Detail = cameraRDD.mapToPair(new PairFunction<Row, String, Row>() {

            /**
             * row.getString(1) 是得到monitor_id 。
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row)  {
                return new Tuple2<String, Row>(row.getString(1),row);
            }
        });
        return monitorId2Detail;
    }
}

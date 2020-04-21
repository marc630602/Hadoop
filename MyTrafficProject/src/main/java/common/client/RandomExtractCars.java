package common.client;

import MockData.MockData;
import com.alibaba.fastjson.JSONObject;
import common.conf.ConfigurationManager;
import common.constant.Constants;
import common.dao.ICarTrackDAO;
import common.dao.IRandomExtractDAO;
import common.dao.ITaskDAO;
import common.dao.factory.DAOFactory;
import common.domain.CarTrack;
import common.domain.RandomExtractCar;
import common.domain.RandomExtractMonitorDetail;
import common.domain.Task;
import common.utils.DateUtils;
import common.utils.ParamUtils;
import common.utils.SparkUtils;
import common.utils.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

/**
 * 8.随机抽取车辆
 */
public class RandomExtractCars {
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
        JSONObject taskParamsJsonObject = JSONObject.parseObject(task.getTaskParams());

        /**
         * 通过params（json字符串）查询monitor_flow_action
         *
         * 获取指定日期内检测的monitor_flow_action中车流量数据，返回JavaRDD<Row>
         */
        JavaRDD<Row> cameraRDD = SparkUtils.getCameraRDDByDateRange(spark, taskParamsJsonObject);

        JavaPairRDD<String, Row> randomExtractCar2DetailRDD = randomExtractCarInfo(sc,taskId,taskParamsJsonObject,cameraRDD);

        /**
         * carTrackRDD<String,String>
         * k:car
         * v:date|carTracker
         * (car,"dateHour=2017-10-18|carTrack=monitor_id,monitor_id,monitor_id...")
         * 相同的车辆会出现在不同的时间段中，那么我们可以追踪在这个日期段中车辆的行驶轨迹
         */
        JavaPairRDD<String, String> carTrackRDD = getCarTrack(taskId,randomExtractCar2DetailRDD);

        /**
         * 将每一辆车的轨迹信息写入到数据库表car_track中
         */
        saveCarTrack2DB(taskId,carTrackRDD);


        System.out.println("all finished...");
        sc.close();
    }

    private static void saveCarTrack2DB(long taskId, JavaPairRDD<String, String> carTrackRDD) {
        //action执行
        carTrackRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,String>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
                //(car,"dateHour=2017-10-18|carTrack=monitor_id,monitor_id,monitor_id...")
                List<CarTrack> carTracks = new ArrayList<>();
                while (iterator.hasNext()) {
                    Tuple2<String, String> tuple = iterator.next();
                    String car = tuple._1;
                    String dateAndCarTrack = tuple._2;
                    String date = StringUtils.getFieldFromConcatString(dateAndCarTrack, "\\|", Constants.FIELD_DATE);
                    String track = StringUtils.getFieldFromConcatString(dateAndCarTrack, "\\|",Constants.FIELD_CAR_TRACK);
                    CarTrack carTrack = new CarTrack(taskId, date,car, track);
                    carTracks.add(carTrack);
                }
                //将车辆的轨迹存入数据库表car_track中
                ICarTrackDAO carTrackDAO = DAOFactory.getCarTrackDAO();
                carTrackDAO.insertBatchCarTrack(carTracks);
            }
        });

    }

    private static JavaPairRDD<String, String> getCarTrack(long taskId,
                                                           JavaPairRDD<String, Row> randomExtractCar2DetailRDD) {
        JavaPairRDD<String, Iterable<Row>> groupByKeyRDD = randomExtractCar2DetailRDD.groupByKey();
        JavaPairRDD<String, String> carTrackRDD = groupByKeyRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {
            //(car,row) -> (car,carTrackDeatil)
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String car = tuple._1;
                Iterator<Row> iterator = tuple._2.iterator();
                List<Row> rows = new ArrayList<>();
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    rows.add(row);
                }

                Collections.sort(rows, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        return DateUtils.before(o1.getString(4), o2.getString(4)) ? -1 : 1;
                    }
                });

                StringBuilder carTrack = new StringBuilder();
                String date = "";
                for (Row row : rows) {
                    carTrack.append("," + row.getString(1));
                    date = row.getString(0);
                }
                ;

                return new Tuple2<String, String>(car, Constants.FIELD_DATE + "=" + date + "|" + Constants.FIELD_CAR_TRACK + "=" + carTrack.substring(1));
            }
        });
        return carTrackRDD;
    }

    private static JavaPairRDD<String, Row> randomExtractCarInfo(JavaSparkContext sc,
                                                                 long taskId,
                                                                 JSONObject params,
                                                                 JavaRDD<Row> cameraRDD) {
        /**
         * key:时间段   value：car
         * dateHourCar2DetailRDD ---- ("dateHour"="2018-01-01_08","car"="京X91427")
         */
        JavaPairRDD<String, String> dateHour2DetaillRDD = cameraRDD.mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String action_time = row.getAs("action_time") + "";
                String dateHour = DateUtils.getDateHour(action_time); //"2020-03-29_10"
                String car = row.getAs("car") + "";
                String key = Constants.FIELD_DATE + "=" + dateHour;
                String value = Constants.FIELD_CAR + "=" + car;
                return new Tuple2<>(key, value);
            }
        }).distinct();

        JavaPairRDD<String, Row> car2DetaillRDD = cameraRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getAs("car") + "", row);
            }
        });

        Map<String, Long> dateHour2CarCount = dateHour2DetaillRDD.countByKey();

        /**
         * 将<dateHour,car_count>这种格式改成格式如下： <date,<Hour,count>>
         */
        HashMap<String, Map<String, Long>> date2HourAndCarCount = new HashMap<>();
        for (Map.Entry<String, Long> entry : dateHour2CarCount.entrySet()) {
            String dateHour = entry.getKey();
            String[] split = dateHour.split(" ");
            String date = split[0];
            String hour = split[1];
            Long carCount = entry.getValue();

            Map<String, Long> hourCountMap = date2HourAndCarCount.get(date);
            if (hourCountMap==null){
                hourCountMap = new HashMap<String,Long>();
                date2HourAndCarCount.put(date,hourCountMap);
            }
            hourCountMap.put(hour,carCount);
        }


        /**
         * 要抽取的车辆数
         * 假设要抽取100辆车
         */
        int extractNums = Integer.parseInt(ParamUtils.getParam(params, Constants.FIELD_EXTRACT_NUM));

        /**
         * 一共抽取100辆车，平均每天应该抽取多少辆车呢？
         * extractNumPerDay = 100 ， dateHourCountMap.size()为有多少不同的天数日期，就是多长
         */
        int extractNumPerDay = extractNums / date2HourAndCarCount.size();

        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<>();

        Random random = new Random();
        //
        // date,map(hour,count)-----date2HourAndCarCount<String,Map<String,Long>>
        for (Map.Entry<String, Map<String, Long>> entry : date2HourAndCarCount.entrySet()) {
            String date = entry.getKey();
            Map<String, Long> hourCountMap = entry.getValue();

            //计算出这一天总的车流量
            long singleDayCarCount = 0L;
            for (Long count : hourCountMap.values()) {
                singleDayCarCount += count;
            }

            //创建随机抽取车辆的index列表的map
            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.computeIfAbsent(date, k -> new HashMap<String, List<Integer>>());

            /**
             * 遍历的是每个小时对应的车流量总数信息
             * hourCountMap  key:hour  value:carCount
             */
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                //当前小时段
                String hour = hourCountEntry.getKey();
                //当前小时段对应的真实的车辆数
                long hourCarCount = hourCountEntry.getValue();

                //计算出这个小时的车流量占总车流量的百分比,然后计算出在这个时间段内应该抽取出来的车辆信息的数量
                int hourExtractNum = (int)(((double)hourCarCount / (double)singleDayCarCount) * extractNumPerDay);

                /**
                 * 如果在这个时间段内抽取的车辆信息数量比这个时间段内的车流量还要多的话，只需要将count的值赋值给hourExtractNum就可以
                 *
                 */
                if(hourExtractNum >= hourCarCount){
                    hourExtractNum = (int)hourCarCount;
                }

                //获取当前小时 存储随机瞅车车辆index的List集合
                List<Integer> extractIndexs = hourExtractMap.computeIfAbsent(hour, k -> new ArrayList<Integer>());

                /**
                 * 生成抽取的car的index，  实际上就是生成一系列的随机数   随机数的范围就是0-count(这个时间段内的车流量) 将这些随机数放入一个list集合中
                 * 那么这里这个随机数的最大值没有超过实际上这个时间点对应的中的车流量总数，这里的list长度也就是要抽取数据个数的大小。
                 * 假设在一天中，7~8点这个时间段总车流量为100，假设我们之前刚刚算出应该在7~8点抽出的车辆数为20
                 * 那么 我们怎么样随机抽取呢？
                 * 1.循环20次
                 * 2.每次循环搞一个0~100的随机数，放入一个list<Integer>中，那么这个list中的每一个元素就是我们这里说的car的index
                 * 3.最后得到一个长度为20的car的indexList<Integer>集合，一会取值，取20个，那么取哪个值呢，就取这里List中的下标对应的car
                 *
                 */
                for(int i = 0 ; i < hourExtractNum ; i++){
                    /**
                     *  50
                     */
                    int index = random.nextInt((int)hourCarCount);
                    while(extractIndexs.contains(index)){
                        index = random.nextInt((int)hourCarCount);
                    }
                    extractIndexs.add(index);
                }
            }
        }

        Broadcast<Map<String, Map<String, List<Integer>>>> bcDateHourExtractMap = sc.broadcast(dateHourExtractMap);

        JavaPairRDD<String, String> extractCarRDD = dateHour2DetaillRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                //将要返回的当前日期当前小时段下抽取出来的车辆集合
                List<Tuple2<String, String>> list = new ArrayList<>();
                //按index下标抽取的这个时间段对应的车辆集合
                List<RandomExtractCar> carRandomExtracts = new ArrayList<>();

                //2020-03-14_10
                String dateHour = tuple._1;
                Iterator<String> cars = tuple._2.iterator();

                String date = dateHour.split("_")[0];
                String hour = dateHour.split("_")[1];

                Map<String, Map<String, List<Integer>>> newDateHourExtractMap = bcDateHourExtractMap.value();
                List<Integer> indexList = newDateHourExtractMap.get(date).get(hour);

                int index = 0;
                while (cars.hasNext()) {
                    String car = cars.next().split("=")[1];
                    if (indexList.contains(index)) {
                        System.out.println("抽取到的车辆 ----" + car);
                        RandomExtractCar carRandomExtract = new RandomExtractCar(taskId, car, date, dateHour);
                        carRandomExtracts.add(carRandomExtract);
                        list.add(new Tuple2<String, String>(car, car));
                    }
                    index++;
                }

                /**
                 * 将抽取出来的车辆信息插入到random_extract_car表中
                 */
                IRandomExtractDAO randomExtractDAO = DAOFactory.getRandomExtractDAO();
                randomExtractDAO.insertBatchRandomExtractCar(carRandomExtracts);
                return list.iterator();
            }
        });

        JavaPairRDD<String, Row> randomExtractCar2Detail =
                extractCarRDD.distinct().join(car2DetaillRDD).mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>, String, Row>() {
            //(car,(car,row)) -> (car,row)
            @Override
            public Iterator<Tuple2<String, Row>> call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
                List<RandomExtractMonitorDetail> randomExtractMonitorDetails = new ArrayList<>();
                List<Tuple2<String, Row>> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();
                    Row row = tuple._2._2;
                    String car = tuple._1;
                    RandomExtractMonitorDetail m = new RandomExtractMonitorDetail(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6));
                    randomExtractMonitorDetails.add(m);
                    list.add(new Tuple2<String, Row>(car, row));
                }
                //将车辆详细信息插入random_extract_car_detail_info表中。
                IRandomExtractDAO randomExtractDAO = DAOFactory.getRandomExtractDAO();
                randomExtractDAO.insertBatchRandomExtractDetails(randomExtractMonitorDetails);
                return list.iterator();
            }
        });

        return randomExtractCar2Detail;
    }
}

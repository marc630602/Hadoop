package transformation.mr.newmember;


import common.EventLogConstants;
import common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import transformation.model.dim.StatsUserDimension;
import transformation.model.value.map.TimeOutPutValue;
import transformation.model.value.reduce.MapWritableValue;
import transformation.mr.TransformerOutPutFormat;
import utils.TimeUtil;

import java.util.Collections;
import java.util.List;

public class NewInstallMemberRunner implements Tool {
    private static final Logger logger = Logger.getLogger(NewInstallMemberRunner.class);
    Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new NewInstallMemberRunner(),args);
        } catch (Exception e) {
            logger.error("解析job异常",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        this.processArgs(conf,args);
        //获取job对象及配置job信息
        Job job = Job.getInstance(conf, "NewInstallUser");
        job.setJarByClass(NewInstallMemberRunner.class);
        TableMapReduceUtil.initTableMapperJob(getScans(conf), NewInstallMemberMapper.class,
                StatsUserDimension.class, TimeOutPutValue.class,job,false);
        job.setReducerClass(NewInstallMemberReducer.class);
        job.setOutputKeyClass(MapWritableValue.class);
        job.setOutputValueClass(StatsUserDimension.class);
        //向mysql输出的类
        job.setOutputFormatClass(TransformerOutPutFormat.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    @Override
    public void setConf(Configuration conf) {
        conf.set("hbase.zookeeper.quorum","hadoop101,hadoop102,hadoop103");
        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("transformer-env.xml");
        conf.set("fs.defaultFS", "hdfs://hadoop101:9000");
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }


    /**
     * 解析参数
     * 参数格式 -d -yyyy-mm-dd
     * @param conf
     * @param args
     */
    private void processArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-d".equals(args[i])) {
                if (i + 1 < args.length) {
                    date = args[++i];
                    break;
                }
            }
        }

        // 要求date格式为: yyyy-MM-dd
        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            // date是一个无效时间数据
            date = TimeUtil.getYesterday(); // 默认时间是昨天
        }
        System.out.println("----------------------" + date);
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }

    /**
     * 从hbase获取符合条件的数据
     * 条件：
     * 1、时间
     * 2、事件类型(en = e_l)
     * 3、获取部分列
     * @param conf
     * @return
     */
    private List<Scan> getScans(Configuration conf) {
        //获取时间
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);

        //创建scan对象,
        Scan scan = new Scan();

        //给scan对象赋值起始位置和结束位置
        long time = TimeUtil.parseString2Long(date);
        byte[] startRow = String.valueOf(time).getBytes();
        byte[] stopRow = String.valueOf(time+GlobalConstants.DAY_OF_MILLISECONDS).getBytes();

        scan.setStartRow(startRow);
        scan.setStopRow(stopRow);

        //创建过滤器对象
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                EventLogConstants.EVENT_LOGS_FAMILY_NAME.getBytes(),
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                EventLogConstants.EventEnum.LAUNCH.alias.getBytes());
        filterList.addFilter(filter1);

        String[] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION,
                EventLogConstants.LOG_COLUMN_NAME_UUID
        };
        filterList.addFilter(this.getFilter(columns));

        //设置过滤器
        scan.setFilter(filterList);
        //设置插入的hbase的表
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,EventLogConstants.HBASE_NAME_EVENT_LOGS.getBytes());

        return Collections.singletonList(scan);
    }

    private Filter getFilter(String[] columns) {
        int length = columns.length;
        byte[][] b = new byte[length][];
        for (int i = 0; i < length ; i++) {
            b[i] = columns[i].getBytes();
        }
        return new MultipleColumnPrefixFilter(b);
    }


}

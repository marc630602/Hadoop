package transformation.mr.activemember;


import common.EventLogConstants;
import common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import transformation.model.dim.StatsUserDimension;
import transformation.model.value.map.TimeOutPutValue;
import transformation.model.value.reduce.MapWritableValue;
import transformation.mr.TransformerOutPutFormat;
import transformation.mr.newinstalluser.NewInstallUserRunner;
import utils.TimeUtil;

import java.util.Arrays;
import java.util.List;

public class ActiveMemberRunner implements Tool {

    private static final Logger logger = Logger.getLogger(ActiveMemberRunner.class);
    Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new ActiveMemberRunner(),args);
        } catch (Exception e) {
            logger.error("解析job异常",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        this.processArgs(args);
        Job job = Job.getInstance(conf,"Active Member");
        job.setJarByClass(ActiveMemberRunner.class);
        TableMapReduceUtil.initTableMapperJob(
                getScans(conf), ActiveMemberMapper.class,
                StatsUserDimension.class, TimeOutPutValue.class,job,false);
        job.setReducerClass(ActiveMemberReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        job.setOutputFormatClass(TransformerOutPutFormat.class);

        return job.waitForCompletion(true) ? 0 : -1 ;
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.set("hbase.zookeeper.quorum","hadoop101,hadoop102,hadoop103");
        configuration.set("fs.defaultFS","hdfs://hadoop101:9000");
        configuration.addResource("output-collector.xml");
        configuration.addResource("query-mapping.xml");
        configuration.addResource("transformer-env.xml");
        this.conf = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    private List<Scan> getScans(Configuration conf) {
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
        long startTime = TimeUtil.parseString2Long(date);
        long endTime = TimeUtil.parseString2Long(date+GlobalConstants.DAY_OF_MILLISECONDS);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startTime));
        scan.setStopRow(Bytes.toBytes(endTime));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                EventLogConstants.EVENT_LOGS_FAMILY_NAME.getBytes(),
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME.getBytes(),
                CompareFilter.CompareOp.EQUAL,
                EventLogConstants.EventEnum.PAGEVIEW.alias.getBytes());
        filterList.addFilter(filter1);

        //服务器时间，平台，浏览器名，浏览器版本，用户id
        String[] columns = new String[] {
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION,
                EventLogConstants.LOG_COLUMN_NAME_UUID,
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME
        };
        filterList.addFilter(this.getFilter(columns));

        scan.setFilter(filterList);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,EventLogConstants.HBASE_NAME_EVENT_LOGS.getBytes());
        return Arrays.asList(scan);
    }

    private Filter getFilter(String[] columns) {
        int length = columns.length;
        byte[][] b = new byte[length][];
        for (int i = 0; i < length; i++) {
            b[i] = columns[i].getBytes();
        }
        return new MultipleColumnPrefixFilter(b);
    }

    private void processArgs(String[] args) {
        String date = null ;
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

}

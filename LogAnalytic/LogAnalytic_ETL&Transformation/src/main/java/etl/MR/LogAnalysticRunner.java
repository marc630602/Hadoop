package etl.MR;

import common.EventLogConstants;
import common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import utils.TimeUtil;

import java.io.IOException;


public class LogAnalysticRunner implements Tool {

    private static final Logger logger = Logger.getLogger(String.valueOf(LogAnalysticRunner.class));
    private Configuration conf = null;

    public static void main(String[] args) {
        try{
            ToolRunner.run(new Configuration(),new LogAnalysticRunner(),args);
        } catch (Exception e){
            logger.error("解析job异常",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        this.processArgs(conf,args);

        Job job = Job.getInstance(conf, "Log ETL");
        // 设置本地job
        job.setJarByClass(LogAnalysticRunner.class);
        job.setMapperClass(LogAnalysticMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置reduce配置
        TableMapReduceUtil.initTableReducerJob(EventLogConstants.HBASE_NAME_EVENT_LOGS, null,job,
                null,null,null,null,false);
        job.setNumReduceTasks(0);

        //设置输入路径
        this.setJobInputPaths(job);
        return job.waitForCompletion(true) ? 0 : -1 ;
    }

    @Override
    public void setConf(Configuration conf) {
        conf.set("fs.defaultFS", "hdfs://hadoop101:9000");
        conf.set("hbase.zookeeper.quorum", "hadoop101");
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    private void processArgs(Configuration conf, String[] args) {
        String date = "";
        for (int i = 0; i <args.length ; i++) {
            if ("-d".equals(args[i])){
                if(i+1<args.length){
                    date += args[++i];
                    break;
                }
            }
        }
        //要求日期是yyyy-mm-dd
        if (StringUtils.isBlank(date) || TimeUtil.isValidateRunningDate(date)){
            //date无效
            date = TimeUtil.getYesterday();
            System.out.println(date);
        }
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES,date);
    }

    private void setJobInputPaths(Job job) {
        Configuration conf = job.getConfiguration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
            // Path inputPath = new Path("/flume/" +
            // TimeUtil.parseLong2String(TimeUtil.parseString2Long(date),
            // "MM/dd/"));
            Path inputPath = new Path("/log/"
                    + TimeUtil.parseLong2String(
                    TimeUtil.parseString2Long(date), "yyyy-MM-dd")
                    + "/");
            if (fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            } else {
                throw new RuntimeException("文件不存在:" + inputPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("设置job的mapreduce输入路径出现异常", e);
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    // nothing
                }
            }
        }
    }
}

package Partition;


import Flow.FlowBean;
import Flow.FlowMapper;
import Flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1. 获取一个Job实例
        Job job = Job.getInstance(new Configuration());

        //2. 设置类路径（classpath）
        job.setJarByClass(PartitionerDriver.class);

        //3. 设置Mapper和Reducer路径
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(MyPartition.class);

        //4. 设置Mapper和Reducer的输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5. 设置输入源和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\input\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\output"));

        //6. 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0:1);

    }
}

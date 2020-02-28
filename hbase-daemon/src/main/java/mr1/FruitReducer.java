package mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {


    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //1.遍历values
        for (Text value : values) {
            //2.获取每一行数据
            String[] fields = value.toString().split("\t");

            //3.构建put对象(申明row key)
            Put put = new Put(Bytes.toBytes(fields[0]));

            //4.给put对象赋值
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("color"),Bytes.toBytes(fields[2]));

            //5.写出
            context.write(NullWritable.get(),put);

        }
    }
}

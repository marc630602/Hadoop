package Flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fileds = value.toString().split("/t");
        phone.set(fileds[1]);
        long upFlow = Long.parseLong(fileds[fileds.length - 3]);
        long downFlow = Long.parseLong(fileds[fileds.length - 2]);
        flow.set(upFlow,downFlow);
        context.write(phone,flow);
    }
}

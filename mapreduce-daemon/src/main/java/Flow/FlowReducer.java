package Flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean sumflow = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpflow = 0;
        long sumDownflow = 0;
        for (FlowBean value : values) {
            sumUpflow += value.getUpFlow();
            sumDownflow += value.getDownFlow();
        }
        sumflow.set(sumUpflow,sumDownflow);
        context.write(key,sumflow);
    }
}

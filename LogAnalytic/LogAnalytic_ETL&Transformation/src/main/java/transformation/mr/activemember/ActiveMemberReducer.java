package transformation.mr.activemember;

import common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import transformation.model.dim.StatsUserDimension;
import transformation.model.value.map.TimeOutPutValue;
import transformation.model.value.reduce.MapWritableValue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ActiveMemberReducer extends Reducer<StatsUserDimension, TimeOutPutValue,StatsUserDimension, MapWritableValue> {

    //创建reduce端输出的value对象
    MapWritableValue mapWritableValue = new MapWritableValue();

    //创建去重set对象
    Set<String> unique = new HashSet<String>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutPutValue> value, Context context) throws IOException, InterruptedException {
        this.unique.clear();

        for (TimeOutPutValue timeOutPutValue : value) {
            unique.add(timeOutPutValue.getId());
        }

        MapWritable map = new MapWritable();
        map.put(new IntWritable(-3),new IntWritable(this.unique.size()));

        String kpiName = key.getStatsCommon().getKpi().getKpiName();
        KpiType kpi = KpiType.valueOfName(kpiName);

        mapWritableValue.setValue(map);
        mapWritableValue.setKpi(kpi);

        context.write(key,mapWritableValue);
    }
}

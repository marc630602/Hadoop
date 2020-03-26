package transformation.mr.newmember;


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

/**
 * 计算new isntall user的reduce类
 * 输入：StatsUserDimension, TimeOutPutValue
 * 输出到MySQL：StatsUserDimension, MapWritableValue
 */
public class NewInstallMemberReducer extends Reducer<StatsUserDimension, TimeOutPutValue,StatsUserDimension, MapWritableValue> {

    //创建reduce端输出的value对象
    MapWritableValue mapWritableValue = new MapWritableValue();

    //创建去重set对象
    Set<String> unique = new HashSet<String>();


    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutPutValue> values, Context context) throws IOException, InterruptedException {
        //清空set集合防止上一个迭代器的值产生影响
        this.unique.clear();

        //遍历迭代器
        for (TimeOutPutValue timeOutPutValue : values) {
            //根据uuid进行去重
            this.unique.add(timeOutPutValue.getId());
        }

        //将map阶段输出到value对象
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-4),new IntWritable(this.unique.size()));
        mapWritableValue.setValue(map);

        //将kpiType信息设置到输出对象
        String kpiName = key.getStatsCommon().getKpi().getKpiName();
        KpiType kpi = KpiType.valueOfName(kpiName);
        mapWritableValue.setKpi(kpi);

        context.write(key,mapWritableValue);
    }
}

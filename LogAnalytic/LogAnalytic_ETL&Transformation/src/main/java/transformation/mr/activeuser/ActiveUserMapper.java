package transformation.mr.activeuser;

import common.DateEnum;
import common.EventLogConstants;
import common.KpiType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import transformation.model.dim.StatsCommonDimension;
import transformation.model.dim.StatsUserDimension;
import transformation.model.dim.base.BrowserDimension;
import transformation.model.dim.base.DateDimension;
import transformation.model.dim.base.KpiDimension;
import transformation.model.dim.base.PlatformDimension;
import transformation.model.value.map.TimeOutPutValue;

import java.io.IOException;
import java.util.List;

public class ActiveUserMapper extends TableMapper<StatsUserDimension, TimeOutPutValue> {

    //定义列族
    byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);

    //定义map端输出key和value的对象
    StatsUserDimension statsUserDimension = new StatsUserDimension();
    TimeOutPutValue timeOutPutValue = new TimeOutPutValue();

    //定义模块维度
    //1）用户基本信息
    KpiDimension activeUser = new KpiDimension(KpiType.ACTIVE_USER.name);
    //2)浏览器信息
    KpiDimension activeUserOfBrowser = new KpiDimension((KpiType.BROWSER_ACTIVE_USER.name));

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //从HBase取出时间、浏览器名字、浏览器版本、平台，用户id信息
        String date = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))));
        String browserName = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME))));
        String browserVersion = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION))));
        String platform = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM))));
        String uuid = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID))));

        DateDimension dateDimension = DateDimension.buildDate(Long.parseLong(date), DateEnum.DAY);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);

        timeOutPutValue.setId(uuid);
        timeOutPutValue.setTime(Long.parseLong(date));

        //拼接维度
        StatsCommonDimension statsCommon = statsUserDimension.getStatsCommon();
        statsCommon.setDate(dateDimension);
        BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");
        for (PlatformDimension platformDimension : platformDimensions) {
            statsCommon.setKpi(activeUser);
            statsCommon.setPlatform(platformDimension);
            statsUserDimension.setBrowser(defaultBrowserDimension);

            context.write(statsUserDimension,timeOutPutValue);
            for (BrowserDimension browserDimension : browserDimensions) {
                statsCommon.setKpi(activeUserOfBrowser);
                statsUserDimension.setBrowser(browserDimension);

                context.write(statsUserDimension,timeOutPutValue);
            }
        }
    }
}

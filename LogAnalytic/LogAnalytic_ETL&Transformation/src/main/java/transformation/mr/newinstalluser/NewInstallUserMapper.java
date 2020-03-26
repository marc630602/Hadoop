package transformation.mr.newinstalluser;

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
/**
 * 自定义的计算新用户的mapper类
 *
 */
public class NewInstallUserMapper extends TableMapper<StatsUserDimension, TimeOutPutValue> {

    //定义列族
    byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);

    //定义map端输出key和value的对象
    StatsUserDimension statsUserDimension = new StatsUserDimension();
    TimeOutPutValue timeOutPutValue = new TimeOutPutValue();

    //定义模块维度
    //1）用户基本信息
    KpiDimension newInstallUsr = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
    //2)浏览器信息
    KpiDimension newInstallUsrOfBrowser = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //构建用户id
        String uuid = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID))));
        //构建时间维度
        String date = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))));
        long time = Long.parseLong(date);
        DateDimension dateDimension = DateDimension.buildDate(time, DateEnum.DAY);

        //构建平台维度
        String platform = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM))));
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        //构建浏览器维度
        String browserName = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME))));
        String browserVersion = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(
                family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION))));
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName,browserVersion);

        //给输出value赋值
        timeOutPutValue.setId(uuid);
        timeOutPutValue.setTime(time);

        //维度组合拼接
        StatsCommonDimension statsCommon = statsUserDimension.getStatsCommon();
        statsCommon.setDate(dateDimension);

        //创建空的浏览器维度
        BrowserDimension defaultBrowser = new BrowserDimension("", "");

        for (PlatformDimension platformDimension : platformDimensions) {
            statsCommon.setKpi(newInstallUsr);
            statsCommon.setPlatform(platformDimension);
            statsUserDimension.setBrowser(defaultBrowser);

            context.write(statsUserDimension,timeOutPutValue);
            for (BrowserDimension browserDimension : browserDimensions) {
                statsCommon.setKpi(newInstallUsrOfBrowser);
                statsUserDimension.setBrowser(browserDimension);

                context.write(statsUserDimension,timeOutPutValue);
            }
        }
    }
}

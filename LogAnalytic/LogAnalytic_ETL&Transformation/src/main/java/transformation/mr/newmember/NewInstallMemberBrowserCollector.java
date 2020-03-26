package transformation.mr.newmember;

import common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import transformation.model.dim.StatsUserDimension;
import transformation.model.dim.base.BaseDimension;
import transformation.model.value.BaseStatsValueWritable;
import transformation.model.value.reduce.MapWritableValue;
import transformation.mr.IOutputCollector;
import transformation.service.IDimensionConverter;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 *
 * @author root
 *
 */
public class NewInstallMemberBrowserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        // 进行强制后获取对应的值
        StatsUserDimension statsUser = (StatsUserDimension) key;
        IntWritable newUserBrowserValue = (IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-4));

        // 进行参数设置
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getBrowser()));
        pstmt.setInt(++i, newUserBrowserValue.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, newUserBrowserValue.get());

        // 添加到batch中
        pstmt.addBatch();
    }
}
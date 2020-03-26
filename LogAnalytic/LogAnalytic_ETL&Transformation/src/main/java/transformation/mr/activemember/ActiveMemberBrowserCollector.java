package transformation.mr.activemember;

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
public class ActiveMemberBrowserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        // 进行强制后获取对应的值
        StatsUserDimension statsUser = (StatsUserDimension) key;
        IntWritable activeMemberBrowserValue = (IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-3));

        // 进行参数设置
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getBrowser()));
        pstmt.setInt(++i, activeMemberBrowserValue.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, activeMemberBrowserValue.get());

        // 添加到batch中
        pstmt.addBatch();
    }

}
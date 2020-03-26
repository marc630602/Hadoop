package etl.MR;


import common.EventLogConstants;
import etl.utils.LoggerUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 1、过滤脏乱差数据
 * 2、IP地址过滤
 * 3、浏览器信息过滤
 * 4、RowKey设计
 */
public class LogAnalysticMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
    private final Logger logger = Logger.getLogger(LogAnalysticMapper.class);
    private int inputRecords, filterRecords, outputRecords; // 主要用于标志，方便查看过滤数据
    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private CRC32 crc32 = new CRC32();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        this.logger.debug("Analyse data of " + value);

        try {
            //解析日志
            Map<String, String> clientInfo = LoggerUtil.handleLog(value.toString());

            //过滤失败日志
            if (clientInfo.isEmpty()){
                this.filterRecords++;
                return ;
            }

            //获取事件名称
            String eventAliasName = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
            EventLogConstants.EventEnum event = EventLogConstants.EventEnum.valueOfAlias(eventAliasName);

            switch (event){
                case LAUNCH:
                case PAGEVIEW:
                case CHARGEREQUEST:
                case CHARGEREFUND:
                case CHARGESUCCESS:
                case EVENT:
                    this.handleData(clientInfo,event,context);
                    break;
                default:
                    this.filterRecords++;
                    this.logger.warn("该事件无法解析"+eventAliasName);
            }
        } catch (Exception e){
            this.filterRecords++;
            this.logger.error("处理数据发生异常"+value,e);
        }
    }

    private void handleData(Map<String, String> clientInfo, EventLogConstants.EventEnum event, Context context)
            throws IOException,InterruptedException{
        String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
        String memberId = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
        String serverTime = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);

        if (StringUtils.isNotBlank(serverTime)){
            //清除浏览器数据
            clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);

            //生成rowkey
            String rowKey = this.generateRowKey(uuid, memberId, event.alias, serverTime);

            Put put = new Put(Bytes.toBytes(rowKey));

            for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
                if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())){
                    put.add(family,Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue()));
                }
            }
            context.write(NullWritable.get(),put);
            this.outputRecords++;
        } else{
            this.filterRecords++;
        }
    }

    private String generateRowKey(String uuid, String memberId, String eventAliasName, String serverTime) {
        StringBuilder sb = new StringBuilder();
        sb.append(serverTime).append("_");

        this.crc32.reset();
        if (StringUtils.isNotBlank(uuid)){
            this.crc32.update(uuid.getBytes());
        }

        if (StringUtils.isNotBlank(memberId)){
            this.crc32.update(memberId.getBytes());
        }
        this.crc32.update(eventAliasName.getBytes());
        sb.append(this.crc32.getValue() % 100000000L);

        return sb.toString();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        logger.info("输入数据:" + this.inputRecords + "；输出数据:" + this.outputRecords
                + "；过滤数据:" + this.filterRecords);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }
}

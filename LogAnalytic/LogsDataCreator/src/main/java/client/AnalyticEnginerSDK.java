package client;


import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AnalyticEnginerSDK {
    // 日志打印对象
    private static final Logger log = Logger.getGlobal();
    //url主体
    private static final String platformName = "java_server";
    public static final String accessURL = "http://hadoop102/log.gif";
    public static final String sdkName = "jdk";
    public static final String version = "1";

    /**
     * 触发订单支付成功事件，发送事件数据到服务器
     *
     * orderId  订单支付id
     * memberId 订单支付会员id
     * @return 如果发送数据成功(加入到发送队列中)，那么返回true；否则返回false(参数异常&添加到发送队列失败).
     */

    public static boolean onChargeSuccess(String orderId,String memberId) {
        try {
            if(isEmpty(orderId)||isEmpty(memberId)){
                log.log(Level.WARNING,"parametre can't be empty");
                return false;
            }

            Map<String,String> data = new HashMap<String,String>();
            data.put("oid",orderId);
            data.put("u_id",memberId);
            data.put("c_time",String.valueOf(System.currentTimeMillis()));
            data.put("ver", version);
            data.put("en", "e_cs");
            data.put("pl", platformName);
            data.put("sdk", sdkName);

            //创建url
            String url = buildUrl(data);
            //将url加入队列
            SendDataMonitor.addSendUrl(url);
            return true;
        } catch (Throwable e){
            log.log(Level.WARNING,"数据发送异常",e);
        }
        return false;
    }

    /**
     * 触发订单退款事件，发送退款数据到服务器
     *
     * orderId
     *            退款订单id
     * memberId
     *            退款会员id
     * @return 如果发送数据成功，返回true。否则返回false。
     */
    public static boolean onChargeRefund(String orderId,String memberId) {
        try {
            if(isEmpty(orderId)||isEmpty(memberId)){
                log.log(Level.WARNING,"parametre can't be empty");
                return false;
            }

            Map<String,String> data = new HashMap<String,String>();
            data.put("oid",orderId);
            data.put("u_id",memberId);
            data.put("c_time",String.valueOf(System.currentTimeMillis()));
            data.put("ver", version);
            data.put("en", "e_cr");
            data.put("pl", platformName);
            data.put("sdk", sdkName);

            //创建url
            String url = buildUrl(data);
            //将url加入队列
            SendDataMonitor.addSendUrl(url);
            return true;
        } catch (Throwable e){
            log.log(Level.WARNING,"数据发送异常",e);
        }
        return false;
    }

    private static String buildUrl(Map<String, String> data) throws UnsupportedEncodingException {
        StringBuilder sb = new StringBuilder();
        sb.append(accessURL).append("?");
        for (Map.Entry<String, String> entry : data.entrySet()) {
            if (isNotEmpty(entry.getKey() )&& isNotEmpty(entry.getValue())){
                sb.append(entry.getKey().trim()).append("=").append(URLEncoder.encode(entry.getValue().trim(),"utf-8")).append("&");
            }
        }
        return sb.substring(0,sb.length()-1);
    }


    private static boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static boolean isNotEmpty(String value) {
        return ! isEmpty(value);
    }



}

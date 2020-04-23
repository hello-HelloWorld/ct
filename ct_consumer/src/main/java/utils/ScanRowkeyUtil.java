package utils;

import sun.net.www.protocol.http.HttpURLConnection;

import javax.swing.plaf.TableUI;
import java.sql.Struct;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/17 13:45
 */
// 该类主要用于根据传入指定的查询时间，生成若干组 startRowKey 和 stopRowKey,相同用户每个月对应一组startRowKey 和 stopRowKey
public class ScanRowkeyUtil {
    private String phoneNum;
    private String startTime;
    private String stopTime;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
    List<String[]> list = new ArrayList<String[]>();
    int indexKey = 0;

    public ScanRowkeyUtil(String phoneNum, String startTime, String stopTime) {
        this.phoneNum = phoneNum;
        this.startTime = startTime;
        this.stopTime = stopTime;
        getRowKeys();
    }

    //01_15837312345_20170101
    //15837312345 2017-01-01 2017-05-01
    private void getRowKeys() {
        //分区数
        int regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
        try {
            Date startDate = sdf1.parse(startTime);
            Date stopDate = sdf1.parse(stopTime);
            //当前rowKey开始时间
            Calendar currentStartCalendar = Calendar.getInstance();
            currentStartCalendar.setTimeInMillis(startDate.getTime());
            //当前rowKey结束时间
            Calendar currentStopCalendar = Calendar.getInstance();
            currentStopCalendar.setTimeInMillis(startDate.getTime());
            currentStopCalendar.add(Calendar.MONTH, 1);

            while (currentStopCalendar.getTimeInMillis() <= stopDate.getTime()) {
                //分区号
                String regionCode = HBaseUtil.getRegionCode(phoneNum, sdf2.format(new Date(currentStartCalendar.getTimeInMillis())), regions);
                //开始的rowKey 01_15837312345_20170101
                String startRowKey = regionCode + "_" + phoneNum + "_" + sdf2.format(new Date(currentStartCalendar.getTimeInMillis()));
                //结束的rowkey
                String endRowKey = regionCode + "_" + phoneNum + "_" + sdf2.format(new Date(currentStopCalendar.getTimeInMillis()));
                String[] rowKeys = {startRowKey, endRowKey};
                list.add(rowKeys);
                currentStartCalendar.add(Calendar.MONTH, 1);
                currentStopCalendar.add(Calendar.MONTH, 1);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    //判断list集合中是否还有下一组rowKey
    public boolean hasNext() {
        if (indexKey < list.size()) {
            return true;
        } else {
            return false;
        }
    }

    //取出list集合中下一组rowKey
    public String[] getRowKEy() {
        String[] rowKey = list.get(indexKey);
        indexKey++;
        return rowKey;
    }
}

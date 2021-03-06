package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.ConnectionInstance;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/16 9:58
 */
//将数据保存到hbase中的类
public class HBaseDao {
    private int regions;//hbase表的分区数
    private String namespace;//表命名空间
    private String tableName;//表名称
    private static Configuration conf;
    private Connection connection;
    private HTable table;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    private List<Put> cacheList = new ArrayList<Put>();

    static {
        conf = HBaseConfiguration.create();
    }

    //在无参构造中进行下面操作
    public HBaseDao() {
        regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
        namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
        tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");

        //创建表,f1列族存放主叫开头的数据，f2存放被叫开头的数据
        try {
            if (!HBaseUtil.isExistTable(conf, tableName)) {
                HBaseUtil.initNameSpace(conf, namespace);
                HBaseUtil.createTable(conf, tableName, regions, "f1", "f2");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * ori数据样式： 18576581848,17269452013,2017-08-14 13:38:31,1761
     * rowkey样式：01_18576581848_20170814133831_17269452013_1_1761
     * HBase表的列：call1  call2   build_time   build_time_ts   flag   duration
     *向表中添加数据
     * @param ori
     */
    public void put(String ori) {

        try {
            if (cacheList.size() == 0) {
                //单例模式获取connection
                connection = ConnectionInstance.getConnection(conf);
                table = (HTable) connection.getTable(TableName.valueOf(tableName));
                // table = new HTable(conf, tableName);过时api
                table.setAutoFlushTo(false);
                table.setWriteBufferSize(2 * 1024 * 1024);
            }

            String[] splitOri = ori.split(",");
            String caller = splitOri[0];
            String callee = splitOri[1];
            String buildTime = splitOri[2];
            String duration = splitOri[3];
            String buildTimeTs = String.valueOf(sdf1.parse(buildTime).getTime());

            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));
            String regionCode = HBaseUtil.getRegionCode(caller, buildTime, regions);

            //生成rowkey
            String rowKey = HBaseUtil.getRowKey(regionCode, caller, buildTimeReplace, callee, "1", duration);
            //向表中插入该数据
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call1"), Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call2"), Bytes.toBytes(callee));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTimeTs));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
            cacheList.add(put);
            if (cacheList.size() >= 30) {
                table.put(cacheList);
                table.flushCommits();
                table.close();
                cacheList.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}

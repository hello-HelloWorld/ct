package utils;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/16 10:18
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.TreeSet;

public class HBaseUtil {

    /*
     * 判断表是否存在
     * */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        admin.close();
        connection.close();
        return result;
    }

    //初始化命名空间
    public static void initNameSpace(Configuration conf, String namespace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("CREATE_TIME", String.valueOf(System.currentTimeMillis()))
                .addConfiguration("AUTHOR", "xiong")
                .build();
        admin.createNamespace(nd);
        admin.close();
        connection.close();
    }

    //创建表
    public static void createTable(Configuration conf, String tableName, int regions, String... cloumnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if (isExistTable(conf, tableName)) {
            return;
        }
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cloumnFamily) {
            htd.addFamily(new HColumnDescriptor(cf));
        }
        //添加协处理器,参数为协处理器的类路径
        htd.addCoprocessor("hbase.CalleeWriteObserver");

        admin.createTable(htd, genSplitKeys(regions));
        admin.close();
        connection.close();
    }

    //创建表的分区键“01|   02| 03| 04|”的二维数组
    //例如： {"00|", "01|", "02|", "03|", "04|", "05|"}
    private static byte[][] genSplitKeys(int regions) {
        //定义一个存放分区键的数组
        String[] keys = new String[regions];
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regions; i++) {
            keys[i] = df.format(i) + "|";
        }

        //分区键的二维数组
        byte[][] splitKeys = new byte[regions][];
        //生成byte[][]类型的分区键的时候，一定要保证分区键是有序的
        TreeSet<byte[]> treeSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regions; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> iterator = treeSet.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            byte[] splitKey = iterator.next();
            splitKeys[index++] = splitKey;
        }
        return splitKeys;
    }

    /**
     * 生成分区号
     * 手机号：15837312345
     * 通话建立时间：2017-01-10 11:20:30 -> 20170110112030
     *
     * @param caller
     * @param buildTime
     * @param regions
     * @return
     */
    public static String getRegionCode(String caller, String buildTime, int regions) {
        //取出手机号后四位
        int len = caller.length();
        String lastPhone = caller.substring(len - 4);
        //取出年月
        String substring = buildTime
                .replaceAll("-", "")
                .replaceAll(" ", "")
                .replaceAll(":", "")
                .substring(0, 6);
        //离散操作1
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(substring);
        //离散操作2
        int y = x.hashCode();
        //生成分区号
        int regionCode = y % regions;
        //格式化分区号
        SimpleDateFormat sdf = new SimpleDateFormat("00");
        return sdf.format(regionCode);
    }

    //  regionCode_call1_buildTime_call2_flag_duration
    public static String getRowKey(String regionCode, String caller, String buildTimeReplace, String callee, String flag, String duration) {
        StringBuilder sb = new StringBuilder();
        StringBuilder rowKey = sb.append(regionCode + "_")
                .append(caller + "_")
                .append(buildTimeReplace + "_")
                .append(callee + "_")
                .append(flag + "_")
                .append(duration);
        return rowKey.toString();
    }
}

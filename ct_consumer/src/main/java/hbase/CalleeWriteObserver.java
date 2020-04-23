package hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/16 14:15
 */
//hbase协处理器
public class CalleeWriteObserver extends BaseRegionObserver {

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    //将穿过来的一条数据，将主叫号码和被叫号码交换位置在存放在hbase表中，相当于一条数据在表中保存了2次
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        //1.获取想要操作的表的名称
        String targetTableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
        //2、获取当前成功Put了数据的表（不一定是我们当前业务想要操作的表）,要进行下面的if判断
        String currentTableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();
        if (!targetTableName.equals(currentTableName)) {
            return;
        }
        //得到原始数据
        //01_158373123456_20170110154530_13737312345_1_0360
        String oriRowKey = Bytes.toString(put.getRow());
        String[] splitOriRowKey = oriRowKey.split("_");

        //如果当前插入的是被叫数据，则直接返回(因为默认提供的数据全部为主叫数据),如果没有此步骤判断则会进入死循环
        String oldFlag = splitOriRowKey[4];
        if ("0".equals(oldFlag)) {
            return;
        }

        int regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
        String caller = splitOriRowKey[1];
        String buildTime = splitOriRowKey[2];
        String callee = splitOriRowKey[3];
        String flag = "0";
        String duration = splitOriRowKey[5];
        String regionCode = HBaseUtil.getRegionCode(callee, buildTime, regions);
        String calleeRowKey = HBaseUtil.getRowKey(regionCode, callee, buildTime, caller, flag, duration);

        //生成时间戳
        String buildTimeTs = "";
        try {
            buildTimeTs = String.valueOf(sdf.parse(buildTime).getTime());
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTimeTs));

        Table table = e.getEnvironment().getTable(TableName.valueOf(targetTableName));
        table.put(calleePut);
        table.close();
    }
}

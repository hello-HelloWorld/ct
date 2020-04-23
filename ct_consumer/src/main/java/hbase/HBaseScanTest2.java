package hbase;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/17 14:36
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import utils.PropertiesUtil;
import utils.ScanRowkeyUtil;

import java.io.IOException;

//本项目采用这个查询hbase表的方案，根据startRowKey和stopRowKey查询，效率高，不用扫描整个表
public class HBaseScanTest2 {

    private static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
    }

    @Test
    public void scanTest() {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            HTable table = (HTable) connection.getTable(TableName.valueOf(PropertiesUtil.getProperty("hbase.calllog.tablename")));
            String phoneNum = "14473548449";
            String startTime = "2017-01-01";
            String stopTime = "2017-05-01";
            Scan scan = new Scan();
            ScanRowkeyUtil scanRowkeyUtil = new ScanRowkeyUtil(phoneNum, startTime, stopTime);
            while (scanRowkeyUtil.hasNext()) {
                String[] rowKey = scanRowkeyUtil.getRowKEy();
                scan.setStartRow(Bytes.toBytes(rowKey[0]));
                scan.setStopRow(Bytes.toBytes(rowKey[1]));
                System.out.println("时间范围" + rowKey[0].substring(15, 21) + "---" +
                        rowKey[1].substring(15, 21));
                ResultScanner scanner = table.getScanner(scan);
                //每一个 rowkey 对应一个 result
                for (Result result : scanner) {
                    //每一个 rowkey 里面包含多个 cell
                    Cell[] cells = result.rawCells();
                    StringBuilder sb = new StringBuilder();
                    sb.append(Bytes.toString(result.getRow())).append(",");
                    for (Cell cell : cells) {
                        sb.append(Bytes.toString(CellUtil.cloneValue(cell))).append(",");
                    }
                    System.out.println(sb.toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

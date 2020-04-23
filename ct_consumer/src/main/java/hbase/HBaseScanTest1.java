package hbase;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/17 11:39
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import utils.HBaseFilterUtil;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;


//扫描查询hbase表的类，在该项目中效率低，弃用
public class HBaseScanTest1 {

    private static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
    }

    @Test
    public void scanTest() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        HTable htable = (HTable) connection.getTable(TableName.valueOf(PropertiesUtil.getProperty("hbase.calllog.tablename")));
        Scan scan = new Scan();
        String startTime = null;
        String endTime = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        try {
            startTime = String.valueOf(sdf.parse("2017-01-1").getTime());
            endTime = String.valueOf(sdf.parse("2017-03-1").getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Filter filter1 = HBaseFilterUtil.gteqFilter("f1", "build_time_ts", startTime);
        Filter filter2 = HBaseFilterUtil.lesseqFilter("f1", "build_time_ts", endTime);
        Filter filter = HBaseFilterUtil.andFilter(filter1, filter2);
        scan.setFilter(filter);
        ResultScanner scanner = htable.getScanner(scan);
        //每一个rowkey对应一个result
        for (Result result : scanner) {
            //每一个rowKey中包含很多cell
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("行：" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        htable.close();
        connection.close();
    }
}

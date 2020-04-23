package driver;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/22 18:16
 */

import kv.key.ComDimension;
import kv.value.CountDurationValue;
import mapper.CountDurationMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import outputformat.MysqlOutputFormat;
import reducer.CountDurationReducer;

import java.io.IOException;

//驱动类
public class CountDurationDriver implements Tool {
    private Configuration conf = null;

    @Override
    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }


    @Override
    public int run(String[] strings) throws Exception {
        //得到conf
        Configuration conf = this.conf;
        //实例化job
        Job job = Job.getInstance(conf);
        job.setJarByClass(CountDurationDriver.class);
        //组装mapper inputFormat
        initHBaseInputConfig(job);
        //组装reducer outputformat
        initReducerOutputConfig(job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void initReducerOutputConfig(Job job) {
        //设置reducer类
        job.setReducerClass(CountDurationReducer.class);
        //设置reducer最终输出类型
        job.setOutputKeyClass(ComDimension.class);
        job.setOutputValueClass(CountDurationValue.class);
        //设置自定义的outputFormat
        job.setOutputFormatClass(MysqlOutputFormat.class);
    }

    private void initHBaseInputConfig(Job job) {
        Connection connection = null;
        Admin admin = null;
        try {
            String tableName = "ns_ct:calllog";
            connection = ConnectionFactory.createConnection(job.getConfiguration());
            admin = connection.getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                throw new RuntimeException("没有找到目标表");
            }
            Scan scan = new Scan();
            //初始化Mapper
            TableMapReduceUtil.initTableMapperJob(
                    tableName,
                    scan,
                    CountDurationMapper.class,
                    ComDimension.class,
                    Text.class,
                    job,
                    true
            );
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
                if (connection != null || !connection.isClosed()) {
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            int status = ToolRunner.run(new CountDurationDriver(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

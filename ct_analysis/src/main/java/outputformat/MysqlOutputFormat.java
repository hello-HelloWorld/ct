package outputformat;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/22 15:27
 */

import converter.DimensionConverterImpl;
import kv.key.ComDimension;
import kv.value.CountDurationValue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.JDBCInstance;
import utils.JDBCUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

//自定义OutputFormat
public class MysqlOutputFormat extends OutputFormat<ComDimension, CountDurationValue> {

    private OutputCommitter outputCommitter = null;

    //返回自定义的recordWriter
    @Override
    public RecordWriter<ComDimension, CountDurationValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //初始化Jdbc连接器对象
        Connection connection = JDBCInstance.getInstance();
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
        return new MysqlRecordWriter(connection);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //输出校验
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (outputCommitter == null) {
            String name = taskAttemptContext.getConfiguration().get(FileOutputFormat.OUTDIR);
            Path outputPath = name == null ? null : new Path(name);
            outputCommitter = new FileOutputCommitter(outputPath, taskAttemptContext);
        }
        return outputCommitter;
    }

    static class MysqlRecordWriter extends RecordWriter<ComDimension, CountDurationValue> {
        private DimensionConverterImpl dci = new DimensionConverterImpl();
        private Connection connection = null;
        private String insertSQL = null;
        private PreparedStatement preparedStatement = null;
        private int count = 0;
        private final int BATCH_SIZE = 500;

        public MysqlRecordWriter(Connection connection) {
            this.connection = connection;
        }

        //真正将分析好的数据写入到mysql
        @Override
        public void write(ComDimension key, CountDurationValue value) throws IOException, InterruptedException {
            try {
                //tb_call
                //id_date_contact, id_date_dimension, id_contact, call_sum, call_duration_sum

                //时间维度对应的id
                int idDateDimension = dci.getDimensionID(key.getDateDimension());
                //联系人电话对应的id
                int idContactDimension = dci.getDimensionID(key.getContactDimension());

                String idDateContact = idDateDimension + "_" + idContactDimension;
                int callSum = Integer.valueOf(value.getCallSum());
                int callDurationSum = Integer.valueOf(value.getCallDurationSum());

                if (insertSQL == null) {
                    insertSQL = "INSERT INTO `tb_call` (`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `id_date_contact` = ?;";
                }

                if (preparedStatement == null) {
                    preparedStatement = connection.prepareStatement(insertSQL);
                }
                //本次sql
                int i = 0;
                preparedStatement.setString(++i, idDateContact);
                preparedStatement.setInt(++i, idDateDimension);
                preparedStatement.setInt(++i, idContactDimension);
                preparedStatement.setInt(++i, callSum);
                preparedStatement.setInt(++i, callDurationSum);
                //无则插入，有则更新的判断依据
                preparedStatement.setString(++i, idDateContact);
                preparedStatement.addBatch();
                count++;
                if (count >= BATCH_SIZE) {
                    preparedStatement.executeUpdate();
                    connection.commit();
                    count = 0;
                    preparedStatement.clearBatch();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                if (preparedStatement != null) {
                    preparedStatement.executeUpdate();
                    this.connection.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                JDBCUtil.close(connection, preparedStatement, null);
            }
        }
    }
}

package converter;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/22 16:33
 */

import kv.base.BaseDimension;
import kv.key.ContactDimension;
import kv.key.DateDimension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.JDBCInstance;
import utils.JDBCUtil;
import utils.LRUCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 1、根据传入的维度数据，得到该数据对应的在表中的主键id
 * ** 做内存缓存，LRUCache
 * 分支
 * -- 缓存中有数据 -> 直接返回id
 * -- 缓存中无数据 ->
 * ** 查询Mysql
 * 分支：
 * -- Mysql中有该条数据 -> 直接返回id -> 将本次读取到的id缓存到内存中
 * -- Mysql中没有该数据  -> 插入该条数据 -> 再次反查该数据，得到id并返回 -> 缓存到内存中
 */
public class DimensionConverterImpl implements DimensionConverter {
    private static final Logger logger = LoggerFactory.getLogger(DimensionConverterImpl.class);
    //对象线程化，用于每个线程管理自己的jdbc连接器
    private ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();
    //构建内存缓存对象
    private LRUCache lruCache = new LRUCache(3000);

    public DimensionConverterImpl() {
        //jvm关闭时，释放资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> JDBCUtil.close(threadLocalConnection.get(), null, null)));
    }

    @Override
    public int getDimensionID(BaseDimension baseDimension) {
        //1、根据传入的维度对象获取对应的主键id，先从LRUCache中获取
        //时间维度：date_dimension_year_month_day, 10
        //联系人维度：contact_dimension_telephone, 12
        String cacheKey = genCacheKey(baseDimension);
        //尝试获取缓存的id
        if (lruCache.containsKey(cacheKey)) {
            return lruCache.get(cacheKey);
        }
        //没有得到缓存id，需要执行select操作
        //sqls包含了1组sql语句：查询和插入
        String[] sqls = null;
        if (baseDimension instanceof DateDimension) {
            sqls = getDateDimensionSQL();
        } else if (baseDimension instanceof ContactDimension) {
            sqls = getContactDimensionSQL();
        } else {
            throw new RuntimeException("没有匹配到对应维度信息");
        }

        //准备对Mysql表进行操作，先查询，有可能再插入
        Connection connection = this.getConnection();
        int id = -1;
        synchronized (this) {
            id = execSql(connection, sqls, baseDimension);
        }
        //将刚查询到的id加入到缓存中
        lruCache.put(cacheKey, id);
        return id;
    }

    /*
     * @param conn      JDBC连接器
     * @param sqls      长度为2，第一个位置为查询语句，第二个位置为插入语句
     * @param dimension 对应维度所保存的数据
     * @return
     */
    private int execSql(Connection connection, String[] sqls, BaseDimension baseDimension) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            //1.查询的preparedStatement
            preparedStatement = connection.prepareStatement(sqls[0]);
            //根据不同的维度，封装不同的SQL语句
            setArguments(preparedStatement, baseDimension);
            //执行查询
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                int result = resultSet.getInt(1);
                //释放资源
                JDBCUtil.close(null, preparedStatement, resultSet);
                return result;
            }
            //释放资源
            JDBCUtil.close(null, preparedStatement, resultSet);

            //2，执行插入，封装插入的sql
            preparedStatement = connection.prepareStatement(sqls[1]);
            setArguments(preparedStatement, baseDimension);
            //执行插入
            preparedStatement.executeUpdate();
            //释放资源
            JDBCUtil.close(null, preparedStatement, null);

            //3.查询的preparedStatement
            preparedStatement = connection.prepareStatement(sqls[0]);
            //根据不同的维度，封装不同的sql
            setArguments(preparedStatement, baseDimension);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            //释放资源
            JDBCUtil.close(connection, preparedStatement, resultSet);
        }
        return -1;
    }

    // 设置SQL语句的具体参数
    private void setArguments(PreparedStatement preparedStatement, BaseDimension baseDimension) {
        int i = 0;
        try {
            if (baseDimension instanceof DateDimension) {
                DateDimension dateDimension = (DateDimension) baseDimension;
                preparedStatement.setString(++i, dateDimension.getYear());
                preparedStatement.setString(++i, dateDimension.getMonth());
                preparedStatement.setString(++i, dateDimension.getDay());
            } else if (baseDimension instanceof ContactDimension) {
                ContactDimension contactDimension = (ContactDimension) baseDimension;
                preparedStatement.setString(++i, contactDimension.getTelephone());
                preparedStatement.setString(++i, contactDimension.getName());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 得到当前线程维护的Connection对象
    private Connection getConnection() {
        Connection connection = null;
        try {
            connection = threadLocalConnection.get();
            if (connection == null || connection.isClosed()) {
                connection = JDBCInstance.getInstance();
                threadLocalConnection.set(connection);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    // 返回联系人表的查询和插入语句
    private String[] getContactDimensionSQL() {
        String query = "SELECT `id` FROM `tb_contacts` WHERE `telephone` = ? AND `name` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_contacts` (`telephone`, `name`) VALUES (?, ?);";
        return new String[]{query, insert};
    }

    // 返回时间表的查询和插入语句
    private String[] getDateDimensionSQL() {
        String query = "SELECT `id` FROM `tb_dimension_date` WHERE `year` = ? AND `month` = ? AND `day` = ? ORDER BY `id`;";
        String insert = "INSERT INTO `tb_dimension_date` (`year`, `month`, `day`) VALUES (?, ?, ?);";
        return new String[]{query, insert};
    }

    //根据维度信息得到维度对应的缓存键
    private String genCacheKey(BaseDimension baseDimension) {
        StringBuilder sb = new StringBuilder();
        if (baseDimension instanceof DateDimension) {
            DateDimension dateDimension = (DateDimension) baseDimension;
            sb.append("date_dimension")
                    .append(dateDimension.getYear())
                    .append(dateDimension.getMonth())
                    .append(dateDimension.getDay());
        } else if (baseDimension instanceof ContactDimension) {
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            sb.append("contact_dimension")
                    .append(contactDimension.getName())
                    .append(contactDimension.getTelephone());
        }
        return sb.toString();
    }
}

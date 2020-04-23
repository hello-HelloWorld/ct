package utils;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/15 17:34
 */

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

//读取配置文件工具类
public class PropertiesUtil {
    public static Properties properties = null;

    static {
        InputStream is = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties");
        properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}

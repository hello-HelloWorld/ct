package utils;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collection;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/17 9:53
 */
//HBase过滤器的工具类，使用过滤器查询扫描hbase表的数据
public class HBaseFilterUtil {

    /**
     * 获得相等过滤器。相当于 SQL 的 [字段] = [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter eqFilter(String cf, String col, String val) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(col), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(val));
        filter.setLatestVersionOnly(true);//当最新版本的值和val相等时，才返回改行
        filter.setFilterIfMissing(true);//如果该行中没有此列，则跳过该行。为false时，如果该行中没有此列，则返回该行。
        return filter;
    }

    /**
     * 获得大于过滤器。相当于 SQL 的 [字段] > [值]
     * // * @param cf  列族名
     *
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter gtFilter(String sf, String col, String val) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(sf), Bytes.toBytes(col), CompareFilter.CompareOp.GREATER, Bytes.toBytes(val));
        filter.setLatestVersionOnly(true);
        filter.setFilterIfMissing(true);
        return filter;
    }

    /**
     * 获得大于等于过滤器。相当于 SQL 的 [字段] >= [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter gteqFilter(String cf, String col, String val) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(col), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(val));
        filter.setLatestVersionOnly(true);
        filter.setFilterIfMissing(true);
        return filter;
    }

    /**
     * 获得小于过滤器。相当于 SQL 的 [字段] < [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter lessFilter(String cf, String col, String val) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(col), CompareFilter.CompareOp.LESS, Bytes.toBytes(val));
        filter.setLatestVersionOnly(true);
        filter.setFilterIfMissing(true);
        return filter;
    }

    /**
     * 获得小于等于过滤器。相当于 SQL 的 [字段] <= [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter lesseqFilter(String cf, String col, String val) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(col), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(val));
        filter.setLatestVersionOnly(true);
        filter.setFilterIfMissing(true);
        return filter;
    }

    /**
     * 获得不等于过滤器。相当于 SQL 的 [字段] != [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter neqFilter(String cf, String col, String val) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(col), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(val));
        filter.setLatestVersionOnly(true);
        filter.setFilterIfMissing(true);
        return filter;
    }

    /**
     * 和过滤器 相当于 SQL的 and
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter andFilter(Filter... filters) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if (filters != null || filters.length > 0) {
            if (filters.length > 1) {
                for (Filter filter : filters) {
                    filterList.addFilter(filter);
                }
            }
            if (filters.length == 1) {
                return filters[0];
            }
        }
        return filterList;
    }

    /**
     * 和过滤器 相当于 SQL 的 的 and
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter andFilter(Collection<Filter> filters) {
        return andFilter(filters.toArray(new Filter[0]));
    }

    /**
     * 或过滤器 相当于 SQL 的 or
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter orFilter(Filter... filters) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        if (filters != null || filters.length > 0) {
            for (Filter filter : filters) {
                filterList.addFilter(filter);
            }
        }
        return filterList;
    }

    /**
     * 非空过滤器 相当于 SQL 的 is not null
     *
     * @param cf  列族
     * @param col 列
     * @return 过滤器
     */
    public static Filter notNullFilter(String cf, String col) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(col), CompareFilter.CompareOp.NOT_EQUAL, new NullComparator());
        filter.setLatestVersionOnly(true);
        filter.setFilterIfMissing(true);
        return filter;
    }

    /**
     * 空过滤器 相当于 SQL 的 is null
     *
     * @param cf  列族
     * @param col 列
     * @return 过滤器
     */
    public static Filter nullFilter(String cf, String col) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(col), CompareFilter.CompareOp.EQUAL, new NullComparator());
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        return filter;
    }

    /**
     * 子字符串过滤器 相当于 SQL 的 like '%[val]%'
     *
     * @param cf  列族
     * @param col 列
     * @param sub 子字符串
     * @return 过滤器
     */
    public static Filter likeFilter(String cf, String col, String sub) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(col), CompareFilter.CompareOp.EQUAL, new SubstringComparator(sub));
        filter.setLatestVersionOnly(true);
        filter.setFilterIfMissing(true);
        return filter;
    }

    /**
     * 正则过滤器 相当于 SQL 的 rlike '[regex]'
     *
     * @param cf    列族
     * @param col   列
     * @param regex 正则表达式
     * @return 过滤器
     */
    public static Filter regexFilter(String cf, String col, String regex) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(col), CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        return filter;
    }
}

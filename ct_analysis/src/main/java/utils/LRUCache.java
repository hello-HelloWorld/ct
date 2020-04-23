package utils;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/22 16:38
 */

import java.util.LinkedHashMap;
import java.util.Map;

// 内存缓存
public class LRUCache extends LinkedHashMap<String, Integer> {
    private static final long serialVersionUID = 1L;
    protected int maxElements;

    public LRUCache(int maxSize) {
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
        return (size() > this.maxElements);
    }
}

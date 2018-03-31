package ru.ifmo.escience.ignite.week2.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;

import static ru.ifmo.escience.ignite.Utils.print;

public class CacheUtils {
    public final static int TOTAL = 100000;

    public static void printCacheStats(Ignite ignite) {
        IgniteCache<Object, Object> cache = ignite.cache("mycache");

        cache.remove("START");

        cache.remove("FINISH");

        print("Primary entries: " + cache.localSize(CachePeekMode.PRIMARY));
        print("Backup entries: " + cache.localSize(CachePeekMode.BACKUP));
    }

    private CacheUtils() {
        // No-op
    }
}

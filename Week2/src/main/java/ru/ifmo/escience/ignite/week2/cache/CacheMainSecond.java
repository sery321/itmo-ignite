package ru.ifmo.escience.ignite.week2.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import static ru.ifmo.escience.ignite.Utils.print;
import static ru.ifmo.escience.ignite.week2.cache.CacheUtils.TOTAL;
import static ru.ifmo.escience.ignite.week2.cache.CacheUtils.printCacheStats;

public class CacheMainSecond {
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("Week2/config/default.xml")) {
            IgniteCache<Object, Object> cache = ignite.cache("mycache");

            print("Waiting for data to arrive...");

            while (!cache.remove("START"))
                Thread.sleep(500);

            int cnt = cache.size();

            print("Cache size: " + cnt);

            for (int i = cnt + 1; i <= TOTAL; i++)
                cache.put(i, new Person(i,i * 3));

            long sum = sumFromCache(cache);

            printCacheStats(ignite);

            cache.put("FINISH", sum);

        }
    }

    private static long sumFromCache(IgniteCache<Object, Object> cache) {
        long result = 0L;
        for (int i = 1; i <= TOTAL; i++) {
            result += ((Person) cache.get(i)).getSalary();
        }
        return result;
    }
}

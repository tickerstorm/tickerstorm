package io.tickerstorm.strategy.util;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;

import com.google.common.collect.FluentIterable;

import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration.Strategy;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

public class CacheManager {

  private static net.sf.ehcache.CacheManager cacheManager = null;

  public static synchronized Cache getInstance(String cache) {

    if (cacheManager == null || !cacheManager.cacheExists(cache)) {
      CacheConfiguration config = new CacheConfiguration().eternal(false).maxBytesLocalHeap(100, MemoryUnit.MEGABYTES)
          .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.FIFO).persistence(new PersistenceConfiguration().strategy(Strategy.NONE));
      config.setName(cache);
      cacheManager = net.sf.ehcache.CacheManager.create();
      cacheManager.addCache(new Cache(config));
    }

    return cacheManager.getCache(cache);
  }

  public static DescriptiveStatistics cacheAsDescriptive(String cache, Field<?> f, int period) {

    String key = buildKey(f, period).toString();
    getInstance(cache).acquireWriteLockOnKey(key);
    getInstance(cache).putIfAbsent(new Element(key, new SynchronizedDescriptiveStatistics(period)));

    SynchronizedDescriptiveStatistics q = (SynchronizedDescriptiveStatistics) getInstance(cache).get(key).getObjectValue();

    if (f.getFieldType().isAssignableFrom(BigDecimal.class))
      q.addValue(((BigDecimal) f.getValue()).doubleValue());

    if (f.getFieldType().isAssignableFrom(Integer.class))
      q.addValue(((Integer) f.getValue()).doubleValue());

    getInstance(cache).releaseWriteLockOnKey(key);
    return q;

  }

  public static Iterable<Field<?>> cacheAsQueue(String cache, Field<?> f, int period) {

    String key = buildKey(f).toString();
    getInstance(cache).acquireWriteLockOnKey(key);
    getInstance(cache).putIfAbsent(new Element(key, new PriorityQueue<>(100, new Comparator<Field<?>>() {

      @Override
      public int compare(Field<?> o1, Field<?> o2) {
        return (MarketData.parseTimestamp(f.getEventId()).compareTo(MarketData.parseTimestamp(o2.getEventId())));
      }

    })));

    PriorityQueue<Field<?>> q = (PriorityQueue<Field<?>>) getInstance(cache).get(key).getObjectValue();
    q.add(f);
    Iterable<Field<?>> i = FluentIterable.from(q).limit(period);
    getInstance(cache).releaseWriteLockOnKey(key);
    return i;

  }

  /**
   * Build a common key for a field
   * 
   * @param f
   * @return
   */
  public static StringBuffer buildKey(Field<?> f) {
    return new StringBuffer(MarketData.parseSource(f.getEventId())).append(MarketData.parseSymbol(f.getEventId()))
        .append(Candle.parseInterval(f.getEventId()));
  }

  /**
   * Build a common key for market data
   * 
   * @param f
   * @return
   */
  public static StringBuffer buildKey(MarketData f) {
    return new StringBuffer(f.getStream()).append("|").append(f.getSymbol()).append("|")
        .append(Candle.parseInterval(f.getEventId()));
  }
  
  /**
   * Build a key factoring in period which makes each time series unique for the period
   * 
   * @param f
   * @param period
   * @return
   */
  private static StringBuffer buildKey(Field<?> f, int period) {

    return buildKey(f).append("p-" + period);

  }

  protected CacheManager() {}

}

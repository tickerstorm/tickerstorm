package io.tickerstorm.common.cache;

import io.tickerstorm.common.collections.SynchronizedIndexedTreeMap;
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

    if (cacheManager == null || !cacheManager.cacheExists(cache.toLowerCase())) {
      CacheConfiguration config = new CacheConfiguration().eternal(false).maxBytesLocalHeap(100, MemoryUnit.MEGABYTES)
          .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.FIFO).persistence(new PersistenceConfiguration().strategy(Strategy.NONE));
      config.setName(cache.toLowerCase());
      cacheManager = net.sf.ehcache.CacheManager.create();
      cacheManager.addCache(new Cache(config));
    }

    return cacheManager.getCache(cache.toLowerCase());
  }

  public static synchronized void destroyInstance(String cache) {
    cacheManager.removeCache(cache.toLowerCase());
  }

  public static SynchronizedIndexedTreeMap<Field<?>> getFieldCache(Field<?> f) {
    return (SynchronizedIndexedTreeMap) CacheManager.getInstance(f.getStream().toLowerCase()).get(CacheManager.buildKey(f).toString()).getObjectValue();
  }

  public static void put(Field<?> f, int maxSize) {

    final String key = CacheManager.buildKey(f).toString();
    Element e = CacheManager.getInstance(f.getStream().toLowerCase()).putIfAbsent(new Element(key, new SynchronizedIndexedTreeMap<Field<?>>(Field.SORT_BY_INSTANTS, maxSize)));

    if (e == null) {
      e = CacheManager.getInstance(f.getStream().toLowerCase()).get(key);
    }

    ((SynchronizedIndexedTreeMap<Field<?>>) e.getObjectValue()).put(f.getTimestamp(), f);

  }

  /**
   * Build a common key for a field
   * 
   * "field" + stream + symbol + field name [+ interval]
   * 
   * @param f
   * @return
   */
  public static StringBuffer buildKey(Field<?> f) {
    return new StringBuffer("field-").append(f.getStream()).append(f.getSymbol()).append(f.getName())
        .append(Candle.parseInterval(f.getEventId()));
  }

  /**
   * Build a common key for market data
   * 
   * stream + symbol + interval
   * 
   * @param f
   * @return
   */
  public static StringBuffer buildKey(MarketData f) {
    return new StringBuffer(f.getStream()).append("|").append(f.getSymbol()).append("|").append(Candle.parseInterval(f.getEventId()));
  }

  protected CacheManager() {}

}

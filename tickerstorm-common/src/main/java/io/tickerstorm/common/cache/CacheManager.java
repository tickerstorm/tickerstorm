package io.tickerstorm.common.cache;

import io.tickerstorm.common.collections.SynchronizedIndexedTreeMap;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration.Strategy;
import net.sf.ehcache.config.SizeOfPolicyConfiguration;
import net.sf.ehcache.config.SizeOfPolicyConfiguration.MaxDepthExceededBehavior;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

public class CacheManager {

  private final static int MAX_ELEMENTS_PER_SERIES = 1000;

  private static net.sf.ehcache.CacheManager cacheManager = null;

  public static synchronized Cache getInstance(String cache) {

    if (cacheManager == null || !cacheManager.cacheExists(cache.toLowerCase())) {
      CacheConfiguration config = new CacheConfiguration().eternal(false).maxBytesLocalHeap(100, MemoryUnit.MEGABYTES)
          .sizeOfPolicy(new SizeOfPolicyConfiguration().maxDepth(1000000).maxDepthExceededBehavior(MaxDepthExceededBehavior.ABORT))
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

  public static SynchronizedIndexedTreeMap<Field<?>> cache(Field<?> f) {

    final String key = CacheManager.buildKey(f).toString();
    SynchronizedIndexedTreeMap<Field<?>> map;

    Element e = CacheManager.getInstance(f.getStream().toLowerCase())
        .putIfAbsent(new Element(key, new SynchronizedIndexedTreeMap<Field<?>>(Field.SORT_BY_INSTANTS, MAX_ELEMENTS_PER_SERIES)));

    if (e == null) {
      e = CacheManager.getInstance(f.getStream().toLowerCase()).get(key);
    }

    map = (SynchronizedIndexedTreeMap<Field<?>>) e.getObjectValue();
    map.put(f.getTimestamp(), f);
    CacheManager.getInstance(f.getStream().toLowerCase()).put(new Element(key, map));

    return map;

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
        .append(Bar.parseInterval(f.getEventId()));
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
    return new StringBuffer(f.getStream()).append("|").append(f.getSymbol()).append("|").append(Bar.parseInterval(f.getEventId()));
  }

  protected CacheManager() {}

}

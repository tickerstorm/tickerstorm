package io.tickerstorm.strategy.util;

import io.tickerstorm.common.entity.Candle;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import net.sf.ehcache.Cache;
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

  /**
   * Build a common key for a field
   * 
   * "field" + stream + symbol + field name [+ interval]
   * 
   * @param f
   * @return
   */
  public static StringBuffer buildKey(Field<?> f) {
    return new StringBuffer("field-").append(f.getStream()).append(f.getSymbol()).append(f.getName()).append(Candle.parseInterval(f.getEventId()));
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

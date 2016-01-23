package io.tickerstorm.strategy.util;

import net.sf.ehcache.Cache;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration.Strategy;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

public class CacheManager {

  public final static String MARKETDATA_CACHE = "md-cache";
  
  private static net.sf.ehcache.CacheManager cacheManager = null;

  public static synchronized net.sf.ehcache.CacheManager getInstance() {

    if (cacheManager == null) {
      CacheConfiguration config = new CacheConfiguration().eternal(false).maxBytesLocalHeap(100, MemoryUnit.MEGABYTES)
          .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.FIFO).persistence(new PersistenceConfiguration().strategy(Strategy.NONE));
      config.setName("md-cache");
      cacheManager = net.sf.ehcache.CacheManager.create();
      cacheManager.addCache(new Cache(config));
    }

    return cacheManager;
  }

  protected CacheManager() { }

}

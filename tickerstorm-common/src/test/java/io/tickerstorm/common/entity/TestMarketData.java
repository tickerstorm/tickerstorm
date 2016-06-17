package io.tickerstorm.common.entity;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMarketData {
  
  /**
   * Test for chronological ordering
   */
  @Test
  public void testComparable(){
    
    List<Candle> cs = new ArrayList<>();
    
    Candle c1 = new Candle();
    c1.timestamp = Instant.now();
    
    Candle c2 = new Candle();
    c2.timestamp = Instant.now().minusMillis(5000);
    
    cs.add(c2);
    cs.add(c1);
    
    Assert.assertEquals(cs.get(0), c2);
    Assert.assertEquals(cs.get(1), c1);
    
    Collections.sort(cs);
    Assert.assertEquals(cs.get(0), c1);
    Assert.assertEquals(cs.get(1), c2);
  }
  
  /**
   * Test for chronological ordering
   */
  @Test
  public void testComparableReverseTimestmap(){
    
    List<Candle> cs = new ArrayList<>();
    
    Candle c1 = new Candle();
    c1.timestamp = Instant.now();
    
    Candle c2 = new Candle();
    c2.timestamp = Instant.now().minusMillis(5000);
    
    cs.add(c2);
    cs.add(c1);
    
    Assert.assertEquals(cs.get(0), c2);
    Assert.assertEquals(cs.get(1), c1);
    
    Collections.sort(cs, MarketData.SORT_BY_TIMESTAMP);
    Assert.assertEquals(cs.get(0), c1);
    Assert.assertEquals(cs.get(1), c2);
  }
  
  /**
   * Test for reverse chronological ordering
   */
  @Test
  public void testComparableByTimestmap(){
    
    List<Candle> cs = new ArrayList<>();
    
    Candle c1 = new Candle();
    c1.timestamp = Instant.now();
    
    Candle c2 = new Candle();
    c2.timestamp = Instant.now().minusMillis(5000);
    
    cs.add(c2);
    cs.add(c1);
    
    Assert.assertEquals(cs.get(0), c2);
    Assert.assertEquals(cs.get(1), c1);
    
    Collections.sort(cs, MarketData.SORT_REVERSE_TIMESTAMP);
    Assert.assertEquals(cs.get(0), c2);
    Assert.assertEquals(cs.get(1), c1);
  }

}

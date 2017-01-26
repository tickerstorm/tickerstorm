/*
 * Copyright (c) 2017, Tickerstorm and/or its affiliates. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     - Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     - Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *     - Neither the name of Tickerstorm or the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 *   IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 *   THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 *   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 *   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.tickerstorm.common.entity;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TestMarketData {
  
  /**
   * Test for chronological ordering
   */
  @Test
  public void testComparable(){
    
    List<Bar> cs = new ArrayList<>();
    
    Bar c1 = new Bar();
    c1.timestamp = Instant.now();
    
    Bar c2 = new Bar();
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
    
    List<Bar> cs = new ArrayList<>();
    
    Bar c1 = new Bar();
    c1.timestamp = Instant.now();
    
    Bar c2 = new Bar();
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
    
    List<Bar> cs = new ArrayList<>();
    
    Bar c1 = new Bar();
    c1.timestamp = Instant.now();
    
    Bar c2 = new Bar();
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

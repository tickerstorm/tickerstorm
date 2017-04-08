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

package org.apache.commons.math3.stat.descriptive;

import com.google.common.base.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Created by kkarski on 2/3/17.
 */
public class ComparableDescriptiveStatistics extends SynchronizedDescriptiveStatistics {

  public final AtomicBoolean full = new AtomicBoolean(false);
  public String test;

  public ComparableDescriptiveStatistics(int windowSize) {
    super(windowSize);
  }

  @Override
  public void addValue(double v) {
    synchronized (full) {
      super.addValue(v);
      full.set(getN() == windowSize);
    }

  }

  public boolean isFull() {
    return full.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(DescriptiveStatistics.class.isAssignableFrom(o.getClass()))) {
      return false;
    }

    DescriptiveStatistics that = (DescriptiveStatistics) o;

    return new EqualsBuilder()
        .append(getGeometricMean(), that.getGeometricMean())
        .append(getKurtosis(), that.getKurtosis())
        .append(getMax(), that.getMax())
        .append(getMin(), that.getMin())
        .append(getN(), that.getN())
        .append(getPopulationVariance(), that.getPopulationVariance())
        .append(getWindowSize(), that.getWindowSize())
        .append(getVariance(), that.getVariance())
        .append(getSum(), that.getSum())
        .append(getSumsq(), that.getSumsq())
        .append(getSkewness(), that.getSkewness())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getGeometricMean())
        .append(getKurtosis())
        .append(getMax())
        .append(getMin())
        .append(getN())
        .append(getPopulationVariance())
        .append(getWindowSize())
        .append(getVariance())
        .append(getSum())
        .append(getSumsq())
        .append(getSkewness())
        .toHashCode();
  }

  @Override
  public synchronized ComparableDescriptiveStatistics copy() {
    ComparableDescriptiveStatistics result =
        new ComparableDescriptiveStatistics(windowSize);
    // No try-catch or advertised exception because arguments are guaranteed non-null
    copy(this, result);
    return result;
  }
}

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

package io.tickerstorm.data.dao.cassandra;

import com.google.common.collect.Sets;
import io.tickerstorm.common.entity.Bar;
import io.tickerstorm.common.entity.Field;
import io.tickerstorm.common.entity.MarketData;
import io.tickerstorm.data.dao.ModelDataDto;
import java.io.Serializable;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.data.cassandra.mapping.Table;

@Table("modeldata")
@SuppressWarnings("serial")
public class CassandraModelDataDto implements Serializable, ModelDataDto {

  public final Set<String> fields = new HashSet<>();
  @org.springframework.data.cassandra.mapping.PrimaryKey
  public ModelDataPrimaryKey primarykey;

  public static CassandraModelDataDto convert(Field<?> field) {

    CassandraModelDataDto dto = new CassandraModelDataDto();

    ModelDataPrimaryKey key = new ModelDataPrimaryKey();
    key.timestamp = Date.from(field.getTimestamp());
    key.date = new BigInteger(dateFormatter.format(field.getTimestamp()));
    key.stream = field.getStream().toLowerCase();
    dto.primarykey = key;
    dto.fields.add(field.serialize());

    return dto;
  }

  public static Set<CassandraModelDataDto> convertFields(Collection<Field<?>> fields) {

    return fields.stream().map(f -> {
      return convert(f);
    }).collect(Collectors.toSet());
  }

  public static Set<CassandraModelDataDto> convertBars(Collection<Bar> bars) {

    Set<CassandraModelDataDto> dtos = Sets.newHashSet();

    bars.stream().map(b -> {
      return b.getFields();
    }).map(f -> {
      return CassandraModelDataDto.convertFields(f);
    }).forEach(s -> {
      dtos.addAll(s);
    });

    return dtos;
  }

  public static CassandraModelDataDto convert(MarketData md) {

    CassandraModelDataDto dto = new CassandraModelDataDto();
    ModelDataPrimaryKey key = new ModelDataPrimaryKey();
    key.timestamp = Date.from(md.getTimestamp());
    LocalDateTime dt = LocalDateTime.ofInstant(md.getTimestamp(), ZoneOffset.UTC);
    key.date = new BigInteger(dateFormatter.format(dt));
    key.stream = md.getStream().toLowerCase();
    dto.primarykey = key;
    md.getFields().forEach(f -> {
      dto.fields.add(f.serialize());
    });

    return dto;
  }

  @Override
  public Set<Field<?>> asFields() {

    Set<Field<?>> fs = new HashSet<>();

    fields.stream().forEach(s -> {
      fs.add(Field.deserialize(s));
    });

    return fs;

  }

}

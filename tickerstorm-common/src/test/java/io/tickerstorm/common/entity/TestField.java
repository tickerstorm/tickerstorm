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

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import org.junit.Test;

public class TestField {

  private final Bar c =
      new Bar("goog", "google", Instant.now(), BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "1m", 1000);

  @Test
  public void testFieldParsing() {

    c.setStream("TestField");
    Map<String, Field<?>> fields = c.getFieldsAsMap();

    Field<Integer> vol = (Field<Integer>) fields.get(Field.Name.VOLUME.field());
    assertEquals(vol.getEventId(), c.getEventId());
    assertEquals(vol.getName(), Field.Name.VOLUME.field());
    assertEquals(vol.getStream(), c.getStream());
    assertEquals(vol.getTimestamp(), c.getTimestamp());
    assertEquals(Field.parseType(vol.serialize()).getName(), vol.getValue().getClass().getName());
    assertEquals(Field.parseField(vol.serialize()), vol.getName());
    assertEquals(Field.parseValue(vol.serialize(), Field.Name.VOLUME.fieldType()), vol.getValue());
    assertEquals(Field.parseStream(vol.serialize()), vol.getStream());
     assertEquals(Field.parseTimestamp(vol.serialize()), c.getTimestamp());
    assertEquals(Field.parseEventId(vol.serialize()), vol.getEventId());

  }

  @Test
  public void testFieldDeserialization() {

    c.setStream("TestField");
    Map<String, Field<?>> fields = c.getFieldsAsMap();

    Field<Integer> vol = (Field<Integer>) fields.get(Field.Name.VOLUME.field());

    String serial = vol.serialize();

    Field<Integer> f = (Field<Integer>) Field.deserialize(serial);

    assertEquals(f.getEventId(), c.getEventId());
    assertEquals(f.getName(), Field.Name.VOLUME.field());
    assertEquals(f.getStream(), c.getStream());
    assertEquals(f.getValue(), c.getVolume());
    assertEquals(f.getFieldType().getName(), Integer.class.getName());
    assertEquals(f.getFieldType(), Field.Name.VOLUME.fieldType());
  }
  
  @Test
  public void testSerializeNullField(){
    
    c.setStream("TestField");
    Map<String, Field<?>> fields = c.getFieldsAsMap();
    Field<Integer> vol = (Field<Integer>) fields.get(Field.Name.VOLUME.field());
    
    Field<Integer> nullField = new BaseField<Integer>(vol, Field.Name.MAX.field() + "-p" + 5, Integer.class);
    String serialized = nullField.serialize();
    
    Field<Integer> newNullField = (Field<Integer>) Field.deserialize(serialized);
    assertEquals(nullField, newNullField);
  }

}

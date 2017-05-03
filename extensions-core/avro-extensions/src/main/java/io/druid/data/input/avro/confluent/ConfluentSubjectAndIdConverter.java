/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.data.input.avro.confluent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.Pair;
import io.druid.data.input.avro.SubjectAndIdConverter;
import org.schemarepo.api.converter.Converter;
import org.schemarepo.api.converter.IdentityConverter;
import org.schemarepo.api.converter.IntegerConverter;

import java.nio.ByteBuffer;

/**
 * This implementation uses a subject name, and an integer as schema id.
 * Before sending avro message to Kafka broker, you need to prepend a magic byte of zero,
 * register the schema to an schema repository, get the schema id, serialized it to 4 bytes and then
 * write it after the magic byte, then you can serialize the avro message. On the reading end, you
 * extract the magic byte, then the 4 bytes of the schema id from raw messages, using the subject and
 * the id you can then look up the avro schema.
 *
 * @see SubjectAndIdConverter
 */
public class ConfluentSubjectAndIdConverter implements SubjectAndIdConverter<String, Integer>
{
  private static final byte MAGIC_BYTE = 0;
  private final String subject;

  @JsonCreator
  public ConfluentSubjectAndIdConverter(@JsonProperty("subject") String subject)
  {
    this.subject = subject;
  }


  @Override
  public Pair<String, Integer> getSubjectAndId(ByteBuffer payload)
  {
    if(payload.get() != MAGIC_BYTE) {
      throw new IllegalArgumentException("The payload did not contain the Confluent magic byte.");
    }
    return new Pair<String, Integer>(subject, payload.getInt());
  }

  @Override
  public void putSubjectAndId(String subject, Integer id, ByteBuffer payload)
  {
    payload.put(MAGIC_BYTE);
    payload.putInt(id);
  }

  @Override
  public Converter<String> getSubjectConverter()
  {
    return new IdentityConverter();
  }

  @Override
  public Converter<Integer> getIdConverter()
  {
    return new IntegerConverter();
  }

  @JsonProperty
  public String getSubject()
  {
    return subject;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConfluentSubjectAndIdConverter converter = (ConfluentSubjectAndIdConverter) o;

    return !(subject != null ? !subject.equals(converter.subject) : converter.subject != null);

  }

  @Override
  public int hashCode()
  {
    return subject != null ? subject.hashCode() : 0;
  }
}

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
package io.druid.data.input.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.parsers.ParseException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.ByteBufferInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class SchemaRepoBasedAvroBytesDecoder<SUBJECT, ID> implements AvroBytesDecoder
{
  private final IAvroSchemaRepository<SUBJECT, ID> schemaRepository;
  private final SubjectAndIdConverter<SUBJECT, ID> subjectAndIdConverter;

  @JsonCreator
  public SchemaRepoBasedAvroBytesDecoder(
      @JsonProperty("subjectAndIdConverter") SubjectAndIdConverter<SUBJECT, ID> subjectAndIdConverter,
      @JsonProperty("schemaRepository") IAvroSchemaRepository<SUBJECT, ID> schemaRepository
  )
  {
    this.subjectAndIdConverter = subjectAndIdConverter;
    this.schemaRepository = schemaRepository;
  }

  @JsonProperty
  public IAvroSchemaRepository<SUBJECT, ID> getSchemaRepository()
  {
    return schemaRepository;
  }

  @JsonProperty
  public SubjectAndIdConverter<SUBJECT, ID> getSubjectAndIdConverter()
  {
    return subjectAndIdConverter;
  }

  @Override
  public GenericRecord parse(ByteBuffer bytes)
  {
    Pair<SUBJECT, ID> subjectAndId = subjectAndIdConverter.getSubjectAndId(bytes);

    Schema schema = null;
    try {
      schema = schemaRepository.getSchema(subjectAndId.lhs, subjectAndId.rhs);

      if(schema == null) {
        throw new ParseException(
                String.format(
                        "Unable to retrieve the schema (%s, %s) from the registry.",
                        subjectAndId.lhs,
                        subjectAndId.rhs
                )
        );
      }
    } catch (IOException e) {
      throw new IllegalStateException(
              String.format(
                      "Unable to retrieve the schema (%s, %s) from the registry.",
                      subjectAndId.lhs,
                      subjectAndId.rhs
              ),
              e
      );
    }
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
    ByteBufferInputStream inputStream = new ByteBufferInputStream(Collections.singletonList(bytes));
    try {
      return reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));
    }
    catch (IOException e) {
      throw new ParseException(e, "Failed to decode avro message!");
    }
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

    SchemaRepoBasedAvroBytesDecoder<?, ?> that = (SchemaRepoBasedAvroBytesDecoder<?, ?>) o;

    if (subjectAndIdConverter != null
        ? !subjectAndIdConverter.equals(that.subjectAndIdConverter)
        : that.subjectAndIdConverter != null) {
      return false;
    }
    return !(schemaRepository != null
             ? !schemaRepository.equals(that.schemaRepository)
             : that.schemaRepository != null);
  }

  @Override
  public int hashCode()
  {
    int result = subjectAndIdConverter != null ? subjectAndIdConverter.hashCode() : 0;
    result = 31 * result + (schemaRepository != null ? schemaRepository.hashCode() : 0);
    return result;
  }
}

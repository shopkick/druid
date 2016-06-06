/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.data.input.avro.schemarepo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.avro.IAvroSchemaRepository;
import org.apache.avro.Schema;
import org.schemarepo.api.TypedSchemaRepository;
import org.schemarepo.api.converter.AvroSchemaConverter;
import org.schemarepo.client.Avro1124RESTRepositoryClient;

import java.io.IOException;

public class Avro1124RESTRepositoryClientWrapper
        extends Avro1124RESTRepositoryClient implements IAvroSchemaRepository<String, Integer>
{
  private final TypedSchemaRepository<Integer, Schema, String> typedRepository;
  private final String url;

  public Avro1124RESTRepositoryClientWrapper(
      @JsonProperty("url") String url,
      @JsonProperty("topic") String topic
  )
  {
    super(url);
    this.url = url;
    Avro1124SubjectAndIdConverter converter = new Avro1124SubjectAndIdConverter(topic);
    typedRepository = new TypedSchemaRepository<Integer, Schema, String>(
            this,
            converter.getIdConverter(),
            new AvroSchemaConverter(false),
            converter.getSubjectConverter()
    );
  }

  @JsonIgnore
  @Override
  public String getStatus()
  {
    return super.getStatus();
  }

  @JsonProperty
  public String getUrl()
  {
    return url;
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

    Avro1124RESTRepositoryClientWrapper that = (Avro1124RESTRepositoryClientWrapper) o;

    return !(url != null ? !url.equals(that.url) : that.url != null);
  }

  @Override
  public int hashCode()
  {
    return url != null ? url.hashCode() : 0;
  }

  @Override
  public Schema getSchema(String subject, Integer id) throws IOException {
    return typedRepository.getSchema(subject, id);
  }
}

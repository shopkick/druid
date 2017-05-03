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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.druid.data.input.avro.IAvroSchemaRepository;
import org.apache.avro.Schema;

import java.io.IOException;

/**
 * A wrapper around the Confluent CachedSchemaRegistryClient exposing the interface
 * needed by Druid.
 */
public class CachedSchemaRepositoryClientWrapper implements IAvroSchemaRepository<String, Integer>
{
  private final CachedSchemaRegistryClient schemaRegistryClient;
  private final String url;
  private final int identityMapCapacity;

  /**
   *
   * @param url The URL of the repository.
   * @param identityMapCapacity The maximum number of versions of a particular schema. Defaults to 1000.
   */
  public CachedSchemaRepositoryClientWrapper(
          @JsonProperty("url") String url,
          @JsonProperty("identityMapCapacity") Integer identityMapCapacity
  )
  {
    if(Strings.isNullOrEmpty(url)) { throw new IllegalArgumentException("The url must be provided."); }
    this.identityMapCapacity = identityMapCapacity != null ? identityMapCapacity : 1000;
    this.url = url;
    schemaRegistryClient = new CachedSchemaRegistryClient(url, identityMapCapacity);
  }

  @JsonProperty
  public String getUrl()
  {
    return url;
  }

  @JsonProperty
  public Integer getIdentityMapCapacity()
  {
    return identityMapCapacity;
  }

  @Override
  public Schema getSchema(String subject, Integer id) throws IOException {
    try {
      return schemaRegistryClient.getByID(id);
    } catch (RestClientException e) {
      throw new IllegalArgumentException(
              String.format(
                      "Unable to get the schema using subject '%s' and id '%s'.",
                      subject,
                      id
              )
      );
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

    CachedSchemaRepositoryClientWrapper that = (CachedSchemaRepositoryClientWrapper) o;

    return url.equals(that.url) && identityMapCapacity == that.identityMapCapacity;
  }

  @Override
  public int hashCode()
  {
    return (url + identityMapCapacity).hashCode();
  }
}

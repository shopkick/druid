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

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A schema registry that maintains its state in memory.
 *
 * Created by benvogan on 6/6/16.
 */
public class InMemoryAvroSchemaRepository implements IAvroSchemaRepository<String, Integer> {
    private final Map<String, Map<Integer, Schema>> schemas = new HashMap<>();

    public InMemoryAvroSchemaRepository() {
    }

    @Override
    public Schema getSchema(String subject, Integer id) throws IOException {
        return schemas.get(subject).get(id);
    }

    /**
     * Registers the schema and returns its id. Registering always generates a new id, even
     * if an identical schema has been registered.
     *
     * @param subject
     * @param schema
     * @return
     */
    public Integer register(String subject, Schema schema) {
        Map<Integer, Schema> versions = schemas.get(subject);
        if(versions == null) {
            versions = new HashMap<>();
            schemas.put(subject, versions);
        }

        Integer id = versions.size() + 1;
        versions.put(id, schema);
        return id;
    }
}

package io.druid.data.input.avro;

import io.druid.data.input.avro.IAvroSchemaRepository;
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

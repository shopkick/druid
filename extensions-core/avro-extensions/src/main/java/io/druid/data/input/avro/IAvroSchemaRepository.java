package io.druid.data.input.avro;

import org.apache.avro.Schema;

import java.io.IOException;

/**
 * The interface implemented by an avro schema repository for use by Druid.
 *
 * @param <SUBJECT> The type of the schema subject.
 * @param <ID> The type of the schema id.
 */
public interface IAvroSchemaRepository<SUBJECT, ID> {
    /**
     * Gets the schema having the given subject and id.
     *
     * @param subject
     * @param id
     * @return
     */
    Schema getSchema(SUBJECT subject, ID id) throws IOException;
}

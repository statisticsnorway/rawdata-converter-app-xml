package no.ssb.rawdata.converter.app.xml.schema;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.avro.Schema;

import java.io.Serializable;

@Value
@Builder(toBuilder = true)
public class SchemaAdapter implements Serializable {

    /**
     * Name of the schema (without any filename suffixes), e.g. remabong
     */
    @NonNull
    private final String schemaName;

    /**
     * Avro schema
     */
    @NonNull
    private final Schema schema;

    /**
     * True if the data element is optional. False means that it is required.
     */
    @Builder.Default
    private final Boolean optional = true;

    /**
     * The source rawdata item name that holds data that can be converted according to this schema
     */
    @NonNull
    private final String rawdataItemName;

    /**
     * Name of the resulting item (in the targetAvroSchema) that will hold the converted data.
     */
    @NonNull
    private final String targetItemName;

    /**
     * Name of the root element of the source document to be converted. This is the element
     * that conversion will start from.
     */
    @NonNull
    private final String rootElementName;

}

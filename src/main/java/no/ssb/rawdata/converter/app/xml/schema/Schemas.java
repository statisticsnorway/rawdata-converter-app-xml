package no.ssb.rawdata.converter.app.xml.schema;

import no.ssb.rawdata.converter.app.xml.XmlRawdataConverter.XmlRawdataConverterException;

import java.util.Set;

import static no.ssb.rawdata.converter.util.AvroSchemaUtil.readAvroSchema;

public class Schemas {

    private static final Set<SchemaAdapter> SCHEMAS;

    static {
        SCHEMAS = Set.of(
          SchemaAdapter.builder()
            .schemaName("rema-bong-v1_0")
            .schema(readAvroSchema("schema/rema-bong-v1_0.avsc"))
            .rawdataItemName("data")
            .targetItemName("data")
            .rootElementName("data")
            .build()
        );
    }

    public static SchemaAdapter getBySchemaDescriptor(SchemaDescriptor schemaSource) {
        SchemaAdapter schemaAdapter = SCHEMAS.stream()
          .filter(schema -> schema.getSchemaName().equalsIgnoreCase(schemaSource.getSchemaName()))
          .findFirst()
          .orElseThrow(() ->
            new SchemaNotFoundException("No schema found for " + schemaSource.getSchemaName()));
        schemaAdapter = merge(schemaAdapter, schemaSource);

        return schemaAdapter;
    }

    private static SchemaAdapter merge(SchemaAdapter schemaAdapter, SchemaDescriptor overrides) {
        SchemaAdapter.SchemaAdapterBuilder builder = schemaAdapter.toBuilder();
        if (overrides.getRawdataItemName() != null) {
            builder.rawdataItemName(overrides.getRawdataItemName());
        }
        if (overrides.getTargetItemName() != null) {
            builder.targetItemName(overrides.getTargetItemName());
        }
        if (overrides.getOptional() != null) {
            builder.optional(overrides.getOptional());
        }
        if (overrides.getRootElementName() != null) {
            builder.rootElementName(overrides.getRootElementName());
        }

        return builder.build();
    }

    public static class SchemaNotFoundException extends XmlRawdataConverterException {
        public SchemaNotFoundException(String msg) {
            super(msg);
        }
    }

}

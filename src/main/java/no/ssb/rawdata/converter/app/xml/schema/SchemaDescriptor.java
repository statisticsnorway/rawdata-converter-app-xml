package no.ssb.rawdata.converter.app.xml.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * This allows for overriding properties in a SchemaAdapter
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SchemaDescriptor {
    public SchemaDescriptor(String schemaName) {
        this.schemaName = schemaName;
    }

    private String schemaName;
    private Boolean optional;
    private String rawdataItemName;
    private String targetItemName;
    private String rootElementName;
}

package no.ssb.rawdata.converter.app.xml;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;
import no.ssb.rawdata.converter.app.xml.schema.SchemaDescriptor;

import java.util.HashSet;
import java.util.Set;

@ConfigurationProperties("rawdata.converter.xml")
@Data
public class XmlRawdataConverterConfig {

    /**
     * Schemas of the expected data elements that the converted data is expected
     * to be compliant with.
     */
    private Set<SchemaDescriptor> dataElements = new HashSet<>();

}
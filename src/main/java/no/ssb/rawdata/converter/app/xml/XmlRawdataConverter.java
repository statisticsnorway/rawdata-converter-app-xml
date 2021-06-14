package no.ssb.rawdata.converter.app.xml;

import lombok.extern.slf4j.Slf4j;
import no.ssb.avro.convert.json.Json;
import no.ssb.avro.convert.json.JsonSettings;
import no.ssb.avro.convert.json.ToGenericRecord;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.converter.app.xml.schema.SchemaAdapter;
import no.ssb.rawdata.converter.app.xml.schema.SchemaDescriptor;
import no.ssb.rawdata.converter.app.xml.schema.Schemas;
import no.ssb.rawdata.converter.core.convert.ConversionResult;
import no.ssb.rawdata.converter.core.convert.ConversionResult.ConversionResultBuilder;
import no.ssb.rawdata.converter.core.convert.RawdataConverter;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.core.schema.AggregateSchemaBuilder;
import no.ssb.rawdata.converter.core.schema.DcManifestSchemaAdapter;
import no.ssb.rawdata.converter.util.AvroSchemaUtil;
import no.ssb.rawdata.converter.util.RawdataMessageAdapter;
import no.ssb.rawdata.converter.util.Xml;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static no.ssb.rawdata.converter.util.RawdataMessageAdapter.posAndIdOf;

@Slf4j
public class XmlRawdataConverter implements RawdataConverter {

    private static final String FIELDNAME_MANIFEST = "manifest";
    private static final String FIELDNAME_COLLECTOR_MANIFEST = "collector";
    private static final String FIELDNAME_CONVERTER_MANIFEST = "converter";
    private static final String FIELDNAME_DATA = "data";

    private final XmlRawdataConverterConfig converterConfig;
    private final ValueInterceptorChain valueInterceptorChain;
    private final Set<String> requiredRawdataItems;

    private Schema manifestSchema;
    private DcManifestSchemaAdapter dcManifestSchemaAdapter;
    private final Schema converterManifestSchema;
    private final Set<SchemaAdapter> dataSchemas;
    private Schema targetAvroSchema;

    public XmlRawdataConverter(XmlRawdataConverterConfig converterConfig, ValueInterceptorChain valueInterceptorChain) {
        this.converterConfig = converterConfig;
        this.valueInterceptorChain = valueInterceptorChain;
        this.converterManifestSchema = AvroSchemaUtil.readAvroSchema("schema/converter-manifest.avsc");

        this.dataSchemas = converterConfig.getDataElements()
          .stream().map(schemaDescriptor -> Schemas.getBySchemaDescriptor(schemaDescriptor))
          .collect(Collectors.toSet());
        if (dataSchemas.isEmpty()) {
            throw new XmlRawdataConverterException("No data elements configured. Make sure to specify at least one target schema (app-config.data-elements[].schema-name)");
        }

        this.requiredRawdataItems = dataSchemas.stream()
          .filter(schema -> !schema.getOptional())
          .map(schema -> schema.getRawdataItemName())
          .collect(Collectors.toSet());

    }

    @Override
    public void init(Collection<RawdataMessage> sampleRawdataMessages) {
        log.info("Determine target avro schema from {}", sampleRawdataMessages);
        RawdataMessage sample = sampleRawdataMessages.stream()
          .findFirst()
          .orElseThrow(() ->
            new XmlRawdataConverterException("Unable to determine target avro schema since no sample rawdata messages were supplied. Make sure to configure `converter-settings.rawdata-samples`")
          );

        RawdataMessageAdapter msg = new RawdataMessageAdapter(sample);
        dcManifestSchemaAdapter = DcManifestSchemaAdapter.of(sample);

        manifestSchema = new AggregateSchemaBuilder("dapla.rawdata.manifest")
          .schema(FIELDNAME_COLLECTOR_MANIFEST, dcManifestSchemaAdapter.getDcManifestSchema())
          .schema(FIELDNAME_CONVERTER_MANIFEST, converterManifestSchema)
          .build();

        String targetNamespace = "dapla.rawdata." + msg.getTopic().orElse("csv");
        AggregateSchemaBuilder targetSchemaBuilder = new AggregateSchemaBuilder(targetNamespace)
          .schema("manifest", manifestSchema);

        dataSchemas.forEach(schema -> {
            targetSchemaBuilder.schema(schema.getTargetItemName(), schema.getSchema());
        });

        targetAvroSchema = targetSchemaBuilder.build();
    }

    public DcManifestSchemaAdapter dcManifestSchemaAdapter() {
        if (dcManifestSchemaAdapter == null) {
            throw new IllegalStateException("dcManifestSchemaAdapter is null. Make sure RawdataConverter#init() was invoked in advance.");
        }

        return dcManifestSchemaAdapter;
    }

    @Override
    public Schema targetAvroSchema() {
        if (targetAvroSchema == null) {
            throw new IllegalStateException("targetAvroSchema is null. Make sure RawdataConverter#init() was invoked in advance.");
        }

        return targetAvroSchema;
    }

    @Override
    public boolean isConvertible(RawdataMessage rawdataMessage) {

        // TODO: See how these checks can be made more generic
        for (SchemaDescriptor schemaDescriptor : converterConfig.getDataElements()) {

            // Only convert rema bongs that contain certain textual strings
            // other elements will fail since the schema does not support them
            if (schemaDescriptor.getSchemaName().startsWith("rema-bong")) {
                String xml = new String(rawdataMessage.get("data"));
                if (xml.contains("RetailTransaction") && xml.contains("POSIdentity") && xml.contains("GTIN")) {
                    return true;
                }
                else {
                    log.info("Skipping non-receipt REMA data at " + posAndIdOf(rawdataMessage));
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public ConversionResult convert(RawdataMessage rawdataMessage) {
        ConversionResultBuilder resultBuilder = ConversionResult.builder(targetAvroSchema, rawdataMessage);
        addManifest(rawdataMessage, resultBuilder);

        dataSchemas.forEach(schema -> {
            if (rawdataMessage.keys().contains(schema.getRawdataItemName())) {
                convertXml(rawdataMessage, resultBuilder, schema);
            }
        });

        return resultBuilder.build();
    }

    void addManifest(RawdataMessage rawdataMessage, ConversionResultBuilder resultBuilder) {
        GenericRecord manifest = new GenericRecordBuilder(manifestSchema)
          .set(FIELDNAME_COLLECTOR_MANIFEST, dcManifestSchemaAdapter().newRecord(rawdataMessage, valueInterceptorChain))
          .set(FIELDNAME_CONVERTER_MANIFEST, converterManifestData())
          .build();

        resultBuilder.withRecord(FIELDNAME_MANIFEST, manifest);
    }

    GenericRecord converterManifestData() {
        Map<String, String> schemaInfo = dataSchemas.stream()
          .collect(Collectors.toMap(
            SchemaAdapter::getTargetItemName,
            SchemaAdapter::getSchemaName
          ));

        return new GenericRecordBuilder(converterManifestSchema)
          .set("schemas", schemaInfo)
          .build();
    }

    void convertXml(RawdataMessage rawdataMessage, ConversionResult.ConversionResultBuilder resultBuilder, SchemaAdapter schemaAdapter) {
        byte[] data = rawdataMessage.get(schemaAdapter.getRawdataItemName());
        try {
            String json = xmlToJson(data);
            JsonSettings jsonSettings = new JsonSettings().enforceCamelCasedKeys(false);
            GenericRecord genericRecord = ToGenericRecord.from(json, schemaAdapter.getSchema(), jsonSettings);
            resultBuilder.withRecord(schemaAdapter.getTargetItemName(), genericRecord);
        }
        catch (Exception e) {
            RawdataMessageAdapter.print(rawdataMessage);
            throw new XmlRawdataConverterException("Error converting xml '" + schemaAdapter.getRawdataItemName() + "' element at " + posAndIdOf(rawdataMessage), e);
        }
    }

    static String xmlToJson(byte[] data) {
        Map<String, Object> map = Xml.toGenericMap(new String(data));
        return Json.from(map);
    }

    public static class XmlRawdataConverterException extends RawdataConverterException {
        public XmlRawdataConverterException(String msg) {
            super(msg);
        }
        public XmlRawdataConverterException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
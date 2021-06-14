package no.ssb.rawdata.converter.app.xml;

import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.converter.app.xml.schema.SchemaDescriptor;
import no.ssb.rawdata.converter.core.convert.ConversionResult;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.test.message.RawdataMessageFixtures;
import no.ssb.rawdata.converter.test.message.RawdataMessages;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled
public class Rema1000RawdataConverterTest {

    private static final String TOPIC = "rema";
    static RawdataMessageFixtures fixtures;

    @BeforeAll
    static void loadFixtures() {
        fixtures = RawdataMessageFixtures.init(TOPIC);
    }

    @Disabled
    @Test
    void shouldConvertRawdataMessages() {
        RawdataMessages messages = fixtures.rawdataMessages(TOPIC);
        XmlRawdataConverterConfig config = new XmlRawdataConverterConfig();
        config.setDataElements(Set.of(new SchemaDescriptor("rema-bong-v1_0")));

        XmlRawdataConverter converter = new XmlRawdataConverter(config, new ValueInterceptorChain());
        converter.init(messages.index().values());
        ConversionResult res = converter.convert(messages.index().get("4"));
    }

    @Disabled
    @Test
    void shouldSkipRawdataMessage() {
        RawdataMessages messages = fixtures.rawdataMessages(TOPIC);
        XmlRawdataConverterConfig config = new XmlRawdataConverterConfig();
        config.setDataElements(Set.of(new SchemaDescriptor("rema-bong-v1_0")));

        XmlRawdataConverter converter = new XmlRawdataConverter(config, new ValueInterceptorChain());
        converter.init(messages.index().values());

        RawdataMessage rawdataMessage = messages.index().get("4");
        assertThat(converter.isConvertible(rawdataMessage)).isFalse();
    }

}

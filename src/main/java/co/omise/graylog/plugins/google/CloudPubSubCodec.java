package co.omise.graylog.plugins.google;

import java.io.UnsupportedEncodingException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;

import org.apache.commons.lang3.StringUtils;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.AbstractCodec;
import org.graylog2.plugin.inputs.codecs.Codec;
import org.graylog2.plugin.journal.RawMessage;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudPubSubCodec extends AbstractCodec {
    public static final String NAME = "CloudPubSubCodec";
    private static final Logger LOG = LoggerFactory.getLogger(CloudPubSubCodec.class);

    private final ObjectMapper objectMapper;

    @Inject
    public CloudPubSubCodec(@Assisted Configuration configuration) {
        super(configuration);
        this.objectMapper = new ObjectMapper().enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS)
                .enable(JsonParser.Feature.IGNORE_UNDEFINED);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @FactoryClass
    public interface Factory extends Codec.Factory<CloudPubSubCodec> {
        @Override
        CloudPubSubCodec create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config extends AbstractCodec.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            return new ConfigurationRequest();
        }

        @Override
        public void overrideDefaultValues(@Nonnull ConfigurationRequest cr) {
        }
    }

    @Nullable
    @Override
    public Message decode(RawMessage rawMessage) {
        DateTime timestamp = DateTime.now();
        String message = null;
        try {
            byte[] payload = rawMessage.getPayload();
            message = new String(payload, "UTF-8");
            final JsonNode node;
            try {
                node = objectMapper.readTree(message);
                // Timestamp.
                JsonNode value = node.path("timestamp");
                if (value.isTextual()) {
                  timestamp = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(value.asText());
                }
                if (LOG.isDebugEnabled()) {
                    value = node.path("insertId");
                    LOG.debug("Insert ID : {}", value.asText());
                }
            } catch (final Exception e) {
                LOG.error("Could not parse JSON, first 400 characters: "
                        + StringUtils.abbreviate(message, 403), e);
                throw new IllegalStateException("JSON is null/could not be parsed (invalid JSON)", e);
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error(e.getMessage());
        }
        if (message == null)
            return null;
        Message result = new Message(message, "gcloud-pubsub", timestamp);
        return result;
    }

}
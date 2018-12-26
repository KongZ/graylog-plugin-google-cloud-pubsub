package co.omise.graylog.plugins.google;

import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.Version;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

public class CloudPubSubMetadata implements PluginMetaData {
    private static final String PLUGIN_PROPERTIES = "co.omise.graylog.plugins.google/graylog-plugin.properties";

    @Override
    public String getUniqueId() {
        return CloudPubSubInputPlugin.class.getCanonicalName();
    }

    @Override
    public String getName() {
        return "Google Cloud Pub/Sub Plugin";
    }

    @Override
    public String getAuthor() {
        return "Omise co.,ltd.";
    }

    @Override
    public URI getURL() {
        return URI.create("https://www.omise.co");
    }

    @Override
    public Version getVersion() {
        return Version.fromPluginProperties(getClass(), PLUGIN_PROPERTIES, "version", Version.from(1, 0, 2));
    }

    @Override
    public String getDescription() {
        return "Receiving message from Cloud Pub/Sub and process with Graylog";
    }

    @Override
    public Version getRequiredVersion() {
        return Version.fromPluginProperties(getClass(), PLUGIN_PROPERTIES, "graylog.version", Version.from(2, 4, 0));
    }

    @Override
    public Set<ServerStatus.Capability> getRequiredCapabilities() {
        return Collections.emptySet();
    }
}

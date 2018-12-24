package co.omise.graylog.plugins.google;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import org.graylog2.plugin.Plugin;
import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.PluginModule;

import java.util.Collection;

@AutoService(Plugin.class)
public class CloudPubSubInputPlugin implements Plugin {
    @Override
    public PluginMetaData metadata() {
        return new CloudPubSubMetadata();
    }

    @Override
    public Collection<PluginModule> modules() {
        return ImmutableSet.<PluginModule>of(new CloudPubSubInputModule());
    }
}

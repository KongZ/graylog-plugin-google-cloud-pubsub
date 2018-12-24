package co.omise.graylog.plugins.google;

import org.graylog2.plugin.PluginModule;

public class CloudPubSubInputModule extends PluginModule {
    @Override
    protected void configure() {
        addTransport("google-cloud-pubsub-transport", CloudPubSubPullTransport.class, CloudPubSubPullTransport.Config.class,
        CloudPubSubPullTransport.Factory.class);
        addMessageInput(CloudPubSubInput.class, CloudPubSubInput.Factory.class);
        addCodec(CloudPubSubCodec.NAME, CloudPubSubCodec.class);
    }
}

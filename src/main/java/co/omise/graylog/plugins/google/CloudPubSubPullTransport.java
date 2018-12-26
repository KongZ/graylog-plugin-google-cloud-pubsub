package co.omise.graylog.plugins.google;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import com.codahale.metrics.MetricSet;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.assistedinject.Assisted;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.NumberField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.inputs.transports.ThrottleableTransport;
import org.graylog2.plugin.inputs.transports.Transport;
import org.graylog2.plugin.journal.RawMessage;
import org.graylog2.plugin.lifecycles.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudPubSubPullTransport extends ThrottleableTransport {
   public static final String NAME = "cloudpubsubpull";
   private static final Logger LOG = LoggerFactory.getLogger(CloudPubSubPullTransport.class);

   public static final String CPS_PROJECT_ID = "project_id";
   public static final String CPS_SUBSCRIPTION_NAME = "subscription_name";
   public static final String CPS_CREDENTIAL_FILE = "credential_file";
   public static final String CPS_NUMBER_OF_MESSAGE = "number_of_message";
   public static final String CPS_MESSAGE_SIZE = "message_size";
   // public static final String CPS_INTERVAL = "interval";
   public static final String CPS_SUBSCRIPTION_TOPIC = "topic";
   public static final String CPS_SOURCE_NAME = "source_name";

   private final Configuration configuration;
   private final LocalMetricRegistry localRegistry;
   private final EventBus serverEventBus;
   private final ServerStatus serverStatus;

   private Subscriber subscriber = null;

   // ############
   // Use BlockingQueue to manage Graylog processing
   // ############
   // private final BlockingQueue<PubsubMessage> blockingQueue = new
   // LinkedBlockingDeque<>();
   // private final ExecutorService executor;
   // private Future<?> pullTaskFuture = null;
   // private volatile boolean running = false;

   // private SubscriberStub subscriberStub = null;
   // private final ScheduledExecutorService executor;
   // private ScheduledFuture<?> pullTaskFuture = null;
   // private volatile boolean paused = true;

   @Inject
   public CloudPubSubPullTransport(@Assisted final Configuration configuration, EventBus serverEventBus,
         ServerStatus serverStatus, LocalMetricRegistry localRegistry) {
      super(serverEventBus, configuration);
      this.configuration = configuration;
      this.localRegistry = localRegistry;
      this.serverEventBus = serverEventBus;
      this.serverStatus = serverStatus;
      // ############
      // Use BlockingQueue to manage Graylog processing
      // ############
      // this.executor = Executors.newSingleThreadExecutor(new
      // ThreadFactoryBuilder().setDaemon(true).setNameFormat("cloud-pubsub-pull%d")
      // .setUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception in Cloud
      // Pub/Sub Pull", e)) .build());

      // this.executor = Executors.newScheduledThreadPool(1,
      // new
      // ThreadFactoryBuilder().setDaemon(true).setNameFormat("cloud-pubsub-pull%d")
      // .setUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception in Cloud
      // Pub/Sub Pull", e))
      // .build());
   }

   @Override
   public void doLaunch(MessageInput input) throws MisfireException {
      serverStatus.awaitRunning(() -> lifecycleStateChange(Lifecycle.RUNNING));
      // listen for lifecycle changes
      serverEventBus.register(this);

      // Asynchronized Pull
      ProjectSubscriptionName projectSubscription = ProjectSubscriptionName.of(configuration.getString(CPS_PROJECT_ID),
            configuration.getString(CPS_SUBSCRIPTION_NAME));
      final String subscriptionTopic = projectSubscription.toString();
      configuration.setString(CPS_SUBSCRIPTION_TOPIC, subscriptionTopic);
      // Instantiate an asynchronous message receiver
      MessageReceiver receiver = new MessageReceiver() {
         @Override
         public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            // handle incoming message, then ack/nack the received message
            // LOG.info("async: [id={}]{}", message.getMessageId(),
            // message.getData().toStringUtf8());
            LOG.info("Pubsub ID : {}", message.getMessageId());
            input.processRawMessage(new RawMessage(message.getData().toByteArray()));
            consumer.ack();

            // if (blockingQueue.offer(message)) {
            // consumer.ack();
            // } else {
            // consumer.nack();
            // }
         }
      };
      try (InputStream credential = new FileInputStream(configuration.getString(CPS_CREDENTIAL_FILE))) {
         ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(1)
               .build();
         FlowControlSettings flowControlSettings = FlowControlSettings.newBuilder()
               .setMaxOutstandingElementCount((long) configuration.getInt(CPS_NUMBER_OF_MESSAGE, 1000))
               .setMaxOutstandingRequestBytes(((long) configuration.getInt(CPS_MESSAGE_SIZE, 20)) * 1024 * 1024) // 20MB
               .build();
         CredentialsProvider credentialsProvider = FixedCredentialsProvider
               .create(ServiceAccountCredentials.fromStream(credential));
         if (subscriber != null && subscriber.isRunning()) {
            try {
               subscriber.stopAsync().awaitTerminated(1, TimeUnit.MINUTES);
            } catch (TimeoutException e) {
               LOG.error("Unable to terminate previous subscriber {}", e.getMessage());
            }
         }
         subscriber = Subscriber.newBuilder(subscriptionTopic, receiver).setExecutorProvider(executorProvider)
               .setFlowControlSettings(flowControlSettings).setCredentialsProvider(credentialsProvider).build();
         subscriber.addListener(new Subscriber.Listener() {
            public void starting() {
               LOG.debug("{} is starting", subscriptionTopic);
            }

            public void running() {
               LOG.debug("{} is running", subscriptionTopic);
            }

            public void stopping(Subscriber.State from) {
               LOG.debug("[state={}] {} is stopping", from.toString(), subscriptionTopic);
            }

            public void terminated(Subscriber.State from) {
               LOG.debug("[state={}] {} is terminated", from.toString(), subscriptionTopic);
            }

            public void failed(Subscriber.State from, Throwable failure) {
               LOG.warn("[state={}] {} is failed ({})", from.toString(), subscriptionTopic, failure.toString());
            }
         }, MoreExecutors.directExecutor());
         subscriber.startAsync().awaitRunning();
      } catch (IOException e) {
         LOG.error("Error while reading credentials {}", e.getMessage());
      }
      // ############
      // Use BlockingQueue to manage Graylog processing
      // ############
      // running = true;
      // executor.submit(new Runnable() {
      // @Override
      // public void run() {
      // while (running) {
      // try {
      // PubsubMessage message = blockingQueue.take();
      // LOG.info("Pubsub ID : {}", message.getMessageId());
      // RawMessage rawMessage = new RawMessage(message.getData().toByteArray());
      // input.processRawMessage(rawMessage);
      // } catch (InterruptedException e) {
      // LOG.error("{}", e.getMessage());
      // }
      // }
      // }
      // });
      // executor.submit(task);

      // ############
      // Synchronized Pull
      // ############
      // try (InputStream credential = new
      // FileInputStream(configuration.getString(CPS_CREDENTIAL_FILE))) {
      // configuration.setString(CPS_SUBSCRIPTION_TOPIC, subscriptionTopic);
      // CredentialsProvider credentialsProvider = FixedCredentialsProvider
      // .create(ServiceAccountCredentials.fromStream(credential));
      // SubscriberStubSettings subscriberStubSettings =
      // SubscriberStubSettings.newBuilder()
      // .setTransportChannelProvider(SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
      // .setMaxInboundMessageSize(configuration.getInt(CPS_MESSAGE_SIZE, 20) << 20)
      // // 20MB
      // .build())
      // .setCredentialsProvider(credentialsProvider).build();
      // subscriberStub = GrpcSubscriberStub.create(subscriberStubSettings);
      // PullRequest pullRequest = PullRequest.newBuilder()
      // .setMaxMessages(configuration.getInt(CPS_NUMBER_OF_MESSAGE,
      // 1000)).setReturnImmediately(false)
      // .setSubscription(subscriptionTopic).build();

      // final Runnable task = () -> {
      // if (paused) {
      // LOG.debug("Message processing paused, not polling HTTP resource {}.",
      // subscriptionTopic);
      // return;
      // }
      // if (isThrottled()) {
      // // this transport won't block, but we can simply skip this iteration
      // LOG.debug("Not polling HTTP resource {} because we are throttled.",
      // subscriptionTopic);
      // }
      // // use pullCallable().futureCall to asynchronously perform this operation
      // PullResponse pullResponse = subscriberStub.pullCallable().call(pullRequest);
      // List<String> ackIds = new ArrayList<>();
      // for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
      // // handle received message
      // // LOG.info("sync: {}", message.getMessage().getData().toStringUtf8());
      // input.processRawMessage(new
      // RawMessage(message.getMessage().getData().toByteArray()));
      // ackIds.add(message.getAckId());
      // }
      // // acknowledge received messages
      // AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
      // .setSubscription(subscriptionTopic).addAllAckIds(ackIds).build();
      // // use acknowledgeCallable().futureCall to asynchronously perform this
      // operation
      // subscriberStub.acknowledgeCallable().call(acknowledgeRequest);
      // };
      // pullTaskFuture = executor.scheduleAtFixedRate(task, 0,
      // configuration.getInt(CPS_INTERVAL),
      // TimeUnit.SECONDS);
      // } catch (IOException e) {
      // LOG.error("Error while reading credentials {}", e.getMessage());
      // } catch (Exception e) {
      // LOG.error(e.getMessage());
      // }
   }

   @Override
   public void doStop() {
      serverEventBus.unregister(this);
      if (this.subscriber != null) {
         subscriber.stopAsync().awaitTerminated();
      }
      // ############
      // Use BlockingQueue to manage Graylog processing
      // ############
      // if (pullTaskFuture != null) {
      // running = false;
      // pullTaskFuture.cancel(false);
      // }

      // if (this.subscriberStub != null) {
      // subscriberStub.shutdown();
      // }
   }

   @Subscribe
   public void lifecycleStateChange(Lifecycle lifecycle) {
      LOG.debug("Lifecycle changed to {}", lifecycle);
      // switch (lifecycle) {
      // case RUNNING:
      // paused = false;
      // break;
      // default:
      // paused = true;
      // }
   }

   @Override
   public void setMessageAggregator(CodecAggregator aggregator) {
      // Not supported.
   }

   @Override
   public MetricSet getMetricSet() {
      return localRegistry;
   }

   @FactoryClass
   public interface Factory extends Transport.Factory<CloudPubSubPullTransport> {
      @Override
      CloudPubSubPullTransport create(Configuration configuration);

      @Override
      Config getConfig();
   }

   @ConfigClass
   public static class Config extends ThrottleableTransport.Config {

      @Override
      public ConfigurationRequest getRequestedConfiguration() {
         final ConfigurationRequest r = super.getRequestedConfiguration();
         r.addField(new TextField(CPS_PROJECT_ID, "Project ID", "", "A Google Cloud project ID",
               ConfigurationField.Optional.NOT_OPTIONAL));

         r.addField(new TextField(CPS_SUBSCRIPTION_NAME, "Subscription Name", "", "A Cloud Pub/Sub subscription name",
               ConfigurationField.Optional.NOT_OPTIONAL));

         r.addField(new TextField(CPS_CREDENTIAL_FILE, "Credential file location", "",
               "A Path to the TLS private key file", ConfigurationField.Optional.OPTIONAL));

         r.addField(new NumberField(CPS_NUMBER_OF_MESSAGE, "Message size", 1000, "Maximum number of message to pull"));
         r.addField(
               new NumberField(CPS_MESSAGE_SIZE, "Message size", 20, "Maximum size of message in Megabytes to pull"));

         // r.addField(new NumberField(CPS_INTERVAL, "Interval", 10, "Pulling interval in
         // seconds",
         // ConfigurationField.Optional.NOT_OPTIONAL));

         // r.addField(new TextField(CPS_SOURCE_NAME, "Source Name", "", "Override source
         // name. (default gcp-pubsub)",
         // ConfigurationField.Optional.OPTIONAL));
         return r;
      }
   }
}
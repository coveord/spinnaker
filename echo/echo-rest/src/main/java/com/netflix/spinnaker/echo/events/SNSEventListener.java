package com.netflix.spinnaker.echo.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spectator.api.Registry;
import com.netflix.spinnaker.echo.api.events.Event;
import com.netflix.spinnaker.echo.api.events.EventListener;
import com.netflix.spinnaker.echo.config.SNSProperties;
import com.netflix.spinnaker.echo.jackson.EchoObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.sns.AmazonSNSExtendedClient;
import software.amazon.sns.SNSExtendedClientConfiguration;

@Component
@ConditionalOnProperty("sns.enabled")
class SNSEventListener implements EventListener, DisposableBean {
  private static final Logger log = LoggerFactory.getLogger(SNSEventListener.class);

  private final ObjectMapper mapper = EchoObjectMapper.getInstance();

  private final SnsClient snsClient = SnsClient.builder().build();
  private final S3Client s3Client = S3Client.builder().build();
  private final AmazonSNSExtendedClient snsExtendedClient;

  private final String topicArn;
  private final Boolean logFullEvents;
  private final Registry registry;

  public SNSEventListener(SNSProperties snsProperties, Registry registry) {
    this.topicArn = snsProperties.getTopicArn();
    String bucketName = snsProperties.getBucketName();
    this.logFullEvents = snsProperties.isLogFullEvents();
    this.registry = registry;

    SNSExtendedClientConfiguration snsExtendedClientConfiguration =
        new SNSExtendedClientConfiguration()
            .withPayloadSupportEnabled(s3Client, bucketName)
            .withAlwaysThroughS3(true);
    this.snsExtendedClient = new AmazonSNSExtendedClient(snsClient, snsExtendedClientConfiguration);
  }

  public void publishMessageToTopic(String message) {
    try {
      PublishResponse result =
          this.snsExtendedClient.publish(
              PublishRequest.builder().topicArn(topicArn).message(message).build());
      log.debug("Message {} published", result.messageId());
      if (this.logFullEvents) {
        log.info(message);
      }
    } catch (SnsException e) {
      log.error(e.getMessage(), e);
      registry.counter("event.send.errors", "exception", e.getClass().getName()).increment();
    }
  }

  @Override
  public void processEvent(Event event) {
    log.info("Processing Event through SNS topic {}", this.topicArn);
    try {
      publishMessageToTopic(mapper.writeValueAsString(event));
    } catch (Exception e) {
      log.error("Failed to serialize event", e);
    }
  }

  @Override
  public void destroy() throws Exception {
    this.snsExtendedClient.close();
    this.snsClient.close();
    this.s3Client.close();
  }
}

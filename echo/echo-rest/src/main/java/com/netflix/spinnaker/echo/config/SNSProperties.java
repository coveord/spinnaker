package com.netflix.spinnaker.echo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Configuration
@ConfigurationProperties(prefix = "sns")
@Validated
@Data
public class SNSProperties {
  private String topicArn;
  private String bucketName;
  private boolean logFullEvents = false;
}

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.orca;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Reproduces PES-4014 against the real Orca image: a late approval PATCH must not revert a manual
 * judgment stage that completed in the meantime. The image must carry {@code Pes4014RaceHooks},
 * which force the interleaving on every run. Without the fix this test fails, and the execution it
 * prints is the incident's hang. Never merge.
 */
public class JudgmentPatchRaceContainerTest extends BaseContainerTest {

  private static final Logger logger =
      LoggerFactory.getLogger(JudgmentPatchRaceContainerTest.class);

  private static final String HOLD_COMPLETION = "pes4014HoldCompletion";
  private static final String WAIT_FOR_COMPLETION = "pes4014WaitForCompletion";
  private static final String JUDGMENT_STAGE = "Judgment";
  private static final Duration STAGE_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration PIPELINE_TIMEOUT = Duration.ofSeconds(90);

  private final HttpClient client = HttpClient.newHttpClient();
  private final ObjectMapper mapper = new ObjectMapper();
  private MySQLContainer<?> mysql;

  // One MySQL and one Orca per test: the hooks' latches only meet inside a single Orca.
  @BeforeEach
  void startOrca() throws Exception {
    mysql =
        new MySQLContainer<>("mysql:8.0.37")
            .withDatabaseName("orca")
            .withUsername("root")
            .withPassword("root")
            .withNetwork(network)
            .withNetworkAliases("pes4014mysql")
            .withInitScript("mysql_init.sql");
    mysql.start();
    orcaContainer
        .dependsOn(mysql)
        .waitingFor(Wait.forHttp("/health").withStartupTimeout(Duration.ofSeconds(150)))
        .withEnv("SPRING_APPLICATION_JSON", orcaConfiguration())
        .start();
    orcaContainer.followOutput(new Slf4jLogConsumer(logger));
  }

  @AfterEach
  void stopOrca() {
    orcaContainer.stop();
    mysql.stop();
  }

  @Test
  void aLateApprovalDoesNotRevertACompletedJudgment() throws Exception {
    String executionId = startPipeline();
    String stageId =
        (String)
            awaitJudgment(executionId, judgment -> "RUNNING".equals(judgment.get("status")))
                .get("id");

    HttpResponse<String> approval =
        patchStage(
            executionId, stageId, Map.of("judgmentStatus", "continue", HOLD_COMPLETION, true));
    assertThat(approval.statusCode()).as("first approval: %s", approval.body()).isEqualTo(200);

    awaitJudgment(
        executionId,
        judgment ->
            "RUNNING".equals(judgment.get("status"))
                && tasksOf(judgment).stream()
                    .allMatch(task -> "SUCCEEDED".equals(task.get("status"))));
    HttpResponse<String> lateApproval =
        patchStage(
            executionId, stageId, Map.of("judgmentStatus", "continue", WAIT_FOR_COMPLETION, true));
    logger.info("late approval answered {}", lateApproval.statusCode());

    Map<String, Object> execution =
        awaitExecution(executionId, snapshot -> !"RUNNING".equals(snapshot.get("status")));
    assertThat(execution.get("status"))
        .as(
            "pipeline status; RUNNING with a RUNNING judgment whose task SUCCEEDED is the PES-4014"
                + " hang: %s",
            describe(execution))
        .isEqualTo("SUCCEEDED");

    HttpResponse<String> approvalAfterCompletion =
        patchStage(executionId, stageId, Map.of("judgmentStatus", "continue"));
    assertThat(approvalAfterCompletion.statusCode())
        .as("approval of a completed judgment: %s", approvalAfterCompletion.body())
        .isEqualTo(409);
  }

  private String orcaConfiguration() throws Exception {
    String jdbcUrl = "jdbc:mysql://pes4014mysql:3306/orca";
    Map<String, Boolean> sqlOnly = Map.of("enabled", true);
    Map<String, Boolean> noRedis = Map.of("enabled", false);
    Map<String, Object> storage = Map.of("sql", sqlOnly, "redis", noRedis);
    return mapper.writeValueAsString(
        Map.of(
            "sql.enabled",
            "true",
            "services.fiat.baseUrl",
            "http://nowhere",
            "sql.connectionPool",
            Map.of("jdbcUrl", jdbcUrl, "user", "orca_service", "password", "0rcaPassw0rd"),
            "redis.enabled",
            "false",
            "sql.migration",
            Map.of("jdbcUrl", jdbcUrl, "user", "orca_migrate", "password", "0rcaPassw0rd"),
            "executionRepository",
            storage,
            "keiko.queue",
            storage,
            "queue.pendingExecutionService",
            storage,
            "monitor.activeExecutions.redis",
            "false"));
  }

  private String startPipeline() throws Exception {
    Map<String, Object> pipeline =
        Map.of(
            "application",
            "pes4014",
            "name",
            "judgment-race",
            "trigger",
            Map.of("type", "manual", "user", "pes-4014-test"),
            // Skips EnabledPipelineValidator's Front50 lookup: this pipeline has no saved config.
            "initialConfig",
            Map.of("enabled", true),
            "stages",
            List.of(
                Map.of(
                    "refId",
                    "1",
                    "requisiteStageRefIds",
                    List.of(),
                    "type",
                    "manualJudgment",
                    "name",
                    JUDGMENT_STAGE),
                Map.of(
                    "refId", "2",
                    "requisiteStageRefIds", List.of("1"),
                    "type", "wait",
                    "name", "After judgment",
                    "waitTime", 1)));
    HttpResponse<String> response =
        send(
            HttpRequest.newBuilder(orcaUri("/orchestrate"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(pipeline))));
    assertThat(response.statusCode()).as("start pipeline: %s", response.body()).isEqualTo(200);
    String reference = (String) readMap(response.body()).get("ref");
    return reference.substring(reference.lastIndexOf('/') + 1);
  }

  private HttpResponse<String> patchStage(
      String executionId, String stageId, Map<String, Object> body) throws Exception {
    return send(
        HttpRequest.newBuilder(orcaUri("/pipelines/" + executionId + "/stages/" + stageId))
            .header("Content-Type", "application/json")
            .method("PATCH", HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body))));
  }

  private Map<String, Object> awaitJudgment(
      String executionId, Predicate<Map<String, Object>> condition) throws Exception {
    Instant deadline = Instant.now().plus(STAGE_TIMEOUT);
    while (true) {
      Map<String, Object> judgment = judgmentOf(getExecution(executionId));
      if (judgment != null && condition.test(judgment)) {
        return judgment;
      }
      assertThat(Instant.now())
          .as("judgment stage never reached the expected state: %s", judgment)
          .isBefore(deadline);
      Thread.sleep(100);
    }
  }

  private Map<String, Object> awaitExecution(
      String executionId, Predicate<Map<String, Object>> condition) throws Exception {
    Instant deadline = Instant.now().plus(PIPELINE_TIMEOUT);
    Map<String, Object> execution = getExecution(executionId);
    while (!condition.test(execution) && Instant.now().isBefore(deadline)) {
      Thread.sleep(500);
      execution = getExecution(executionId);
    }
    return execution;
  }

  private Map<String, Object> getExecution(String executionId) throws Exception {
    HttpResponse<String> response =
        send(HttpRequest.newBuilder(orcaUri("/pipelines/" + executionId)).GET());
    assertThat(response.statusCode()).as("get execution: %s", response.body()).isEqualTo(200);
    return readMap(response.body());
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> stagesOf(Map<String, Object> execution) {
    return (List<Map<String, Object>>) execution.get("stages");
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> tasksOf(Map<String, Object> stage) {
    return (List<Map<String, Object>>) stage.getOrDefault("tasks", List.of());
  }

  private static Map<String, Object> judgmentOf(Map<String, Object> execution) {
    return stagesOf(execution).stream()
        .filter(stage -> JUDGMENT_STAGE.equals(stage.get("name")))
        .findFirst()
        .orElse(null);
  }

  private static String describe(Map<String, Object> execution) {
    return stagesOf(execution).stream()
        .map(
            stage ->
                stage.get("name")
                    + "="
                    + stage.get("status")
                    + " tasks="
                    + tasksOf(stage).stream()
                        .map(task -> task.get("name") + ":" + task.get("status"))
                        .collect(Collectors.joining(",")))
        .collect(Collectors.joining("; "));
  }

  private URI orcaUri(String path) {
    return URI.create(
        "http://" + orcaContainer.getHost() + ":" + orcaContainer.getFirstMappedPort() + path);
  }

  private HttpResponse<String> send(HttpRequest.Builder request) throws Exception {
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  private Map<String, Object> readMap(String json) throws Exception {
    return mapper.readValue(json, new TypeReference<>() {});
  }
}

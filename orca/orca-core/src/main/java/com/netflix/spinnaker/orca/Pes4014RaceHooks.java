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

import com.netflix.spinnaker.orca.api.pipeline.models.StageExecution;
import com.netflix.spinnaker.orca.api.pipeline.models.TaskExecution;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test-only hooks that force the PES-4014 interleaving on every run: a stage PATCH reads a stage
 * after its task completed, the stage completion is written, then the PATCH writes its copy. They
 * do nothing unless the stage context or the PATCH body carries one of the keys below. The latches
 * live in memory, so Orca must run as a single instance. Never merge.
 */
public final class Pes4014RaceHooks {

  public static final String HOLD_COMPLETION = "pes4014HoldCompletion";
  public static final String WAIT_FOR_COMPLETION = "pes4014WaitForCompletion";

  // Their sum stays under the queue's ack timeout (1 min on Redis), or the message is redelivered.
  private static final Duration PATCH_READ_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration COMPLETION_WRITE_TIMEOUT = Duration.ofSeconds(20);
  private static final Duration PATCH_WRITE_TIMEOUT = Duration.ofSeconds(20);

  private static final Logger log = LoggerFactory.getLogger(Pes4014RaceHooks.class);
  private static final Map<String, CountDownLatch> latches = new ConcurrentHashMap<>();

  private Pes4014RaceHooks() {}

  public static void beforeCompletionWrite(StageExecution stage) {
    if (!isSet(stage.getContext().get(HOLD_COMPLETION))) {
      return;
    }
    log.info("PES-4014 hook: completion of stage {} waits for the PATCH to read it", stage.getId());
    await(stage.getId(), "patch-read", PATCH_READ_TIMEOUT);
  }

  public static void afterCompletionWrite(StageExecution stage) {
    if (!isSet(stage.getContext().get(HOLD_COMPLETION))) {
      return;
    }
    log.info(
        "PES-4014 hook: completion wrote stage {} as {}, waiting for the PATCH to write",
        stage.getId(),
        stage.getStatus());
    signal(stage.getId(), "completion-written");
    await(stage.getId(), "patch-written", PATCH_WRITE_TIMEOUT);
  }

  public static void afterPatchRead(StageExecution stage, Map<String, Object> patchBody) {
    if (!isSet(patchBody.get(WAIT_FOR_COMPLETION))) {
      return;
    }
    log.info(
        "PES-4014 hook: PATCH read stage {} as {} with tasks {}",
        stage.getId(),
        stage.getStatus(),
        stage.getTasks().stream()
            .map(TaskExecution::getStatus)
            .map(String::valueOf)
            .collect(Collectors.joining(",")));
    signal(stage.getId(), "patch-read");
    if (!await(stage.getId(), "completion-written", COMPLETION_WRITE_TIMEOUT)) {
      log.info(
          "PES-4014 hook: the completion of stage {} could not be written while the PATCH held"
              + " the stage",
          stage.getId());
    }
  }

  public static void afterPatchWrite(String stageId, Map<String, Object> patchBody) {
    if (!isSet(patchBody.get(WAIT_FOR_COMPLETION))) {
      return;
    }
    log.info("PES-4014 hook: PATCH of stage {} returned", stageId);
    signal(stageId, "patch-written");
  }

  private static boolean isSet(Object value) {
    return Boolean.TRUE.equals(value) || "true".equals(value);
  }

  private static CountDownLatch latch(String stageId, String event) {
    return latches.computeIfAbsent(stageId + ":" + event, key -> new CountDownLatch(1));
  }

  private static void signal(String stageId, String event) {
    latch(stageId, event).countDown();
  }

  private static boolean await(String stageId, String event, Duration timeout) {
    try {
      boolean happened = latch(stageId, event).await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      if (!happened) {
        log.warn("PES-4014 hook: no {} on stage {} within {}", event, stageId, timeout);
      }
      return happened;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}

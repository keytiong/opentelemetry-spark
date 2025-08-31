/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2023,2024 Key Tiong TAN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.opentelemetry.javaagent.instrumentation.spark;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.logs.LoggerProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.SpanNameExtractor;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.spark.executor.Executor;
import org.apache.spark.scheduler.*;

public class ApacheSparkSingletons {

  public static final OpenTelemetry OPEN_TELEMETRY = GlobalOpenTelemetry.get();

  public static final String INSTRUMENTATION_NAME = "apache-spark";

  public static final PropertiesTextMapAccessor PROPERTIES_TEXT_MAP_ACCESSOR =
      new PropertiesTextMapAccessor();

  public static final Tracer TRACER =
      OPEN_TELEMETRY.getTracer("io.opentelemetry.apache-spark", "0.11.0-SNAPSHOT");

  public static final LoggerProvider LOGGER_PROVIDER = OPEN_TELEMETRY.getLogsBridge();

  private static Instrumenter<TaskDescription, Object> TASK_RUNNER_INSTRUMENTER = null;

  private static DAGScheduler DAG_SCHEDULER = null;

  private static final SpanNameExtractor<TaskDescription> TASK_SPAN_NAME_EXTRACTOR =
      taskDescription -> "spark_task";

  private static final AttributeKey<Long> SPARK_STAGE_ID_ATTR_KEY =
      AttributeKey.longKey("spark.stage_id");
  private static final AttributeKey<Long> SPARK_STAGE_ATTEMPT_NUMBER_ATTR_KEY =
      AttributeKey.longKey("spark.stage_attempt_number");
  private static final AttributeKey<Long> SPARK_TASK_ID_ATTR_KEY =
      AttributeKey.longKey("spark.task_id");

  private static final AttributeKey<Long> SPARK_TASK_ATTEMPT_NUMBER_ATTR_KEY =
      AttributeKey.longKey("spark.task_attempt_number");

  private static final AttributesExtractor<TaskDescription, Object> TASK_ATTRIBUTES_EXTRACTOR =
      new AttributesExtractor<TaskDescription, Object>() {
        @Override
        public void onStart(
            AttributesBuilder attributes, Context parentContext, TaskDescription request) {
          attributes.put(SPARK_TASK_ID_ATTR_KEY, request.taskId());
          attributes.put(SPARK_TASK_ATTEMPT_NUMBER_ATTR_KEY, Long.valueOf(request.attemptNumber()));
        }

        @Override
        public void onEnd(
            AttributesBuilder attributes,
            Context context,
            TaskDescription request,
            @Nullable Object response,
            @Nullable Throwable error) {}
      };

  private static Field taskDescriptionAccessor = null;

  private static final Map<Integer, Context> JOB_CONTEXT_REGISTRY = new ConcurrentHashMap<>();

  private static final Map<Integer, Context> STAGE_CONTEXT_REGISTRY = new ConcurrentHashMap<>();

  private static void initTaskRunnerInstrumenter() {

    TASK_RUNNER_INSTRUMENTER =
        Instrumenter.builder(OPEN_TELEMETRY, INSTRUMENTATION_NAME, TASK_SPAN_NAME_EXTRACTOR)
            .addAttributesExtractor(TASK_ATTRIBUTES_EXTRACTOR)
            .buildInstrumenter();
  }

  public static void registerDagScheduler(DAGScheduler dagScheduler) {
    DAG_SCHEDULER = dagScheduler;
  }

  public static Stage findStage(Integer stageId) {
    return DAG_SCHEDULER.stageIdToStage().get(stageId).get();
  }

  public static ActiveJob findJob(Integer jobId) {
    return DAG_SCHEDULER.jobIdToActiveJob().get(jobId).get();
  }

  public static String applicationName() {
    if (DAG_SCHEDULER != null) {
      return DAG_SCHEDULER.sc().appName();
    }
    return null;
  }

  public static void storeJobContext(Integer jobId, Context context) {
    JOB_CONTEXT_REGISTRY.put(jobId, context);
  }

  public static Context getJobContext(Integer jobId) {
    return JOB_CONTEXT_REGISTRY.get(jobId);
  }

  public static void removeJobContext(Integer jobId) {
    JOB_CONTEXT_REGISTRY.remove(jobId);
  }

  public static void storeStageContext(Integer stageId, Context context) {
    STAGE_CONTEXT_REGISTRY.put(stageId, context);
  }

  public static Context getStageContext(Integer stageId) {
    return STAGE_CONTEXT_REGISTRY.get(stageId);
  }

  public static void removeStageContext(Integer stageId) {
    STAGE_CONTEXT_REGISTRY.remove(stageId);
  }

  public static Instrumenter<TaskDescription, Object> taskRunnerInstrumenter() {
    if (TASK_RUNNER_INSTRUMENTER == null) {
      initTaskRunnerInstrumenter();
    }
    return TASK_RUNNER_INSTRUMENTER;
  }

  public static TaskDescription getTaskDescription(Executor.TaskRunner taskRunner)
      throws IllegalAccessException {
    if (taskDescriptionAccessor == null) {
      Class taskRunnerClass = org.apache.spark.executor.Executor.TaskRunner.class;
      for (Field f : taskRunnerClass.getDeclaredFields()) {
        if (f.getType().equals(TaskDescription.class)) {
          taskDescriptionAccessor = f;
        }
      }
      taskDescriptionAccessor.setAccessible(true);
    }

    return (TaskDescription) taskDescriptionAccessor.get(taskRunner);
  }

  private ApacheSparkSingletons() {}

  public static Context createStageContext(Integer stageId) {

    Stage stage = ApacheSparkSingletons.findStage(stageId);
    Integer jobId = stage.firstJobId();
    if (jobId != null) {
      StageInfo stageInfo = stage.latestInfo();
      Context jobContext = ApacheSparkSingletons.getJobContext(jobId);
      SpanBuilder builder =
          ApacheSparkSingletons.TRACER
              .spanBuilder("spark_stage")
              .setParent(jobContext)
              .setAttribute(SPARK_STAGE_ID_ATTR_KEY, Long.valueOf(stageId))
              .setAttribute(
                  SPARK_STAGE_ATTEMPT_NUMBER_ATTR_KEY, Long.valueOf(stageInfo.attemptNumber()));

      Long submissionTime = (Long) stageInfo.submissionTime().get();
      if (submissionTime != null) {
        builder.setStartTimestamp(submissionTime, TimeUnit.MILLISECONDS);
      }
      Integer[] jobIds = new Integer[stage.jobIds().size()];
      stage.jobIds().copyToArray(jobIds);
      for (Integer jid : jobIds) {
        if (!Objects.equals(jid, jobId)) {
          Context jcontext = ApacheSparkSingletons.getJobContext(jid);
          if (jcontext != null) {
            Span s = Span.fromContext(jcontext);
            builder.addLink(s.getSpanContext());
          }
        }
      }

      Span stageSpan = builder.startSpan();
      Context stageContext = stageSpan.storeInContext(jobContext);
      ApacheSparkSingletons.storeStageContext(stageId, stageContext);
      return stageContext;
    } else {
      return null;
    }
  }
}

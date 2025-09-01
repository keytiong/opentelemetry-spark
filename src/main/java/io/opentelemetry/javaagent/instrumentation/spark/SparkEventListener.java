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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import java.util.concurrent.TimeUnit;
import org.apache.spark.scheduler.*;

public class SparkEventListener {

  private static final AttributeKey<Long> SPARK_JOB_ID_ATTR_KEY =
      AttributeKey.longKey("spark.job_id");

  private static final AttributeKey<Long> SPARK_STAGE_ID_ATTR_KEY =
      AttributeKey.longKey("spark.stage_id");

  private static final AttributeKey<Long> SPARK_STAGE_ATTEMPT_NUMBER_ATTR_KEY =
      AttributeKey.longKey("spark.stage_attempt_number");

  public static void onApplicationStart(SparkListenerApplicationStart event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  public static void onApplicationEnd(SparkListenerApplicationEnd event) {
    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  public static void onJobStart(SparkListenerJobStart event) {

    Integer jobId = event.jobId();

    Context parentContext = ApacheSparkSingletons.getJobContext(jobId);
    if (parentContext == null) {
      parentContext = Context.current();
    }

    Span jobSpan =
        ApacheSparkSingletons.TRACER
            .spanBuilder("spark_job")
            .setAttribute(SPARK_JOB_ID_ATTR_KEY, Long.valueOf(jobId))
            .setParent(parentContext)
            .setStartTimestamp(event.time(), TimeUnit.MILLISECONDS)
            .startSpan();
    Context jobContext = jobSpan.storeInContext(parentContext);

    ApacheSparkSingletons.storeJobContext(jobId, jobContext);

    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  public static void onJobEnd(SparkListenerJobEnd event) {

    Integer jobId = event.jobId();
    Context jobContext = ApacheSparkSingletons.getJobContext(jobId);

    Span jobSpan = Span.fromContext(jobContext);

    JobResult jobResult = event.jobResult();

    if (jobResult instanceof JobSucceeded$) {
      jobSpan.setStatus(StatusCode.OK);
    } else if (jobResult instanceof JobFailed) {
      JobFailed errorResult = (JobFailed) jobResult;
      jobSpan.recordException(errorResult.exception());
      jobSpan.setStatus(StatusCode.ERROR);
    }

    jobSpan.end(event.time(), TimeUnit.MILLISECONDS);
    ApacheSparkSingletons.removeJobContext(jobId);

    SparkEventLogger.emitSparkEvent(event, event.time());
  }

  public static void onStageSubmitted(SparkListenerStageSubmitted event) {
    StageInfo stageInfo = event.stageInfo();
    int stageId = stageInfo.stageId();
    Long submissionTime = (Long) stageInfo.submissionTime().get();
    ApacheSparkSingletons.createStageContext(stageId);
    SparkEventLogger.emitSparkEvent(event, submissionTime);
  }

  private static void onStageCompleted(SparkListenerStageCompleted event) {
    StageInfo stageInfo = event.stageInfo();
    Integer stageId = stageInfo.stageId();
    Context stageContext = ApacheSparkSingletons.getStageContext(stageId);

    Long completionTime = (Long) stageInfo.completionTime().get();

    if (stageContext != null) {
      Span span = Span.fromContext(stageContext);
      String status = stageInfo.getStatusString();
      if ("failed".equals(status)) {
        span.setStatus(StatusCode.ERROR, stageInfo.failureReason().get());
      } else if ("succeeded".equals(status)) {
        span.setStatus(StatusCode.OK);
      }
      span.end(completionTime, TimeUnit.MILLISECONDS);
      ApacheSparkSingletons.removeStageContext(stageId);
    }

    SparkEventLogger.emitSparkEvent(event, completionTime);
  }

  private static void onTaskStart(SparkListenerTaskStart event) {
    SparkEventLogger.emitSparkEvent(event, event.taskInfo().launchTime());
  }

  private static void onTaskEnd(SparkListenerTaskEnd event) {
    SparkEventLogger.emitSparkEvent(event, event.taskInfo().finishTime());
  }

  private static void onOtherEvent(SparkListenerEvent event) {
    SparkEventLogger.emitSparkEvent(event);
  }

  public static void handleSparkListenerEvent(SparkListenerEvent event) {
    if (event instanceof SparkListenerApplicationStart) {
      SparkEventListener.onApplicationStart((SparkListenerApplicationStart) event);
    } else if (event instanceof SparkListenerApplicationEnd) {
      SparkEventListener.onApplicationEnd((SparkListenerApplicationEnd) event);
    } else if (event instanceof SparkListenerJobStart) {
      SparkEventListener.onJobStart((SparkListenerJobStart) event);
    } else if (event instanceof SparkListenerJobEnd) {
      SparkEventListener.onJobEnd((SparkListenerJobEnd) event);
    } else if (event instanceof SparkListenerStageSubmitted) {
      SparkEventListener.onStageSubmitted((SparkListenerStageSubmitted) event);
    } else if (event instanceof SparkListenerStageCompleted) {
      SparkEventListener.onStageCompleted((SparkListenerStageCompleted) event);
    } else if (event instanceof SparkListenerTaskStart) {
      SparkEventListener.onTaskStart((SparkListenerTaskStart) event);
    } else if (event instanceof SparkListenerTaskEnd) {
      SparkEventListener.onTaskEnd((SparkListenerTaskEnd) event);
    } else {
      SparkEventListener.onOtherEvent(event);
    }
  }
}

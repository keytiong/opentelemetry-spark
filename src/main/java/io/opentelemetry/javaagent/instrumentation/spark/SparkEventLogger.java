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
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import java.util.concurrent.TimeUnit;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.EventLoggingListener$;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerEvent;

public class SparkEventLogger {

  private static final Logger SPARK_EVENT_LOGGER =
      ApacheSparkSingletons.LOGGER_PROVIDER.get(SparkEventLogger.class.getName());

  private static final AttributeKey<String> EVENT_NAME_ATTR_KEY =
      AttributeKey.stringKey("event.name");

  public static void emitSparkEvent(SparkListenerEvent event) {

    emitSparkEvent(event, null);
  }

  public static void emitSparkEvent(SparkListenerEvent event, Long timestamp) {

    SparkListenerEvent evt;
    long ts;

    if (event instanceof SparkListenerEnvironmentUpdate) {
      evt = redactEvent((SparkListenerEnvironmentUpdate) event);
    } else {
      evt = event;
    }

    String eventJsonString = JsonProtocol.sparkEventToJsonString(evt);

    String eventName = String.format("spark.%s", event.getClass().getSimpleName());

    if (timestamp == null) {
      Long evtTime = EventTimeAccessor.getEventTime(event);
      if (evtTime != null) {
        ts = evtTime;
      } else {
        ts = System.currentTimeMillis();
      }
    } else {
      ts = timestamp;
    }

    if (eventJsonString != null) {
      SPARK_EVENT_LOGGER
          .logRecordBuilder()
          .setTimestamp(ts, TimeUnit.MILLISECONDS)
          .setSeverity(Severity.INFO)
          .setAttribute(EVENT_NAME_ATTR_KEY, eventName)
          .setBody(eventJsonString)
          .emit();
    }
  }

  private static SparkListenerEnvironmentUpdate redactEvent(SparkListenerEnvironmentUpdate event) {
    SparkConf sparkConf = ApacheSparkSingletons.sparkConf();
    EventLoggingListener$ listener = EventLoggingListener$.MODULE$;
    return listener.redactEvent(sparkConf, event);
  }
}

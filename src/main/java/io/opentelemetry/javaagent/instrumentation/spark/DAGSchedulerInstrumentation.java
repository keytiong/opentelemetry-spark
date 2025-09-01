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

import static io.opentelemetry.javaagent.instrumentation.spark.ApacheSparkSingletons.registerDagScheduler;
import static net.bytebuddy.asm.Advice.*;
import static net.bytebuddy.matcher.ElementMatchers.*;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorage;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.scheduler.*;
import org.apache.spark.storage.BlockManagerMaster;
import org.apache.spark.util.Clock;

public class DAGSchedulerInstrumentation implements TypeInstrumentation {
  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.spark.scheduler.DAGScheduler");
  }

  @Override
  public void transform(TypeTransformer typeTransformer) {

    typeTransformer.applyAdviceToMethod(
        named("submitJob"), this.getClass().getName() + "$SubmitJobAdvice");

    typeTransformer.applyAdviceToMethod(
        isConstructor()
            .and(takesArgument(0, named("org.apache.spark.SparkContext")))
            .and(takesArgument(1, named("org.apache.spark.scheduler.TaskScheduler")))
            .and(takesArgument(2, named("org.apache.spark.scheduler.LiveListenerBus")))
            .and(takesArgument(3, named("org.apache.spark.MapOutputTrackerMaster")))
            .and(takesArgument(4, named("org.apache.spark.storage.BlockManagerMaster")))
            .and(takesArgument(5, named("org.apache.spark.SparkEnv")))
            .and(takesArgument(6, named("org.apache.spark.util.Clock"))),
        this.getClass().getName() + "$Interceptor");
  }

  public static class Interceptor {

    @OnMethodExit()
    public static void exit(
        @This DAGScheduler dagScheduler,
        @Argument(0) SparkContext sparkContext,
        @Argument(1) TaskScheduler taskScheduler,
        @Argument(2) LiveListenerBus liveListenerBus,
        @Argument(3) MapOutputTrackerMaster mapOutputTrackerMaster,
        @Argument(4) BlockManagerMaster blockManagerMaster,
        @Argument(5) SparkEnv sparkEnv,
        @Argument(6) Clock clock) {

      registerDagScheduler(dagScheduler);
    }
  }

  public static class SubmitJobAdvice {

    @OnMethodExit()
    public static void exit(@Return JobWaiter<?> jobWaiter) {
      Context parentContext = ContextStorage.get().current();
      if (parentContext != null) {
        int jobId = jobWaiter.jobId();
        ApacheSparkSingletons.storeJobContext(jobId, parentContext);
      }
    }
  }
}

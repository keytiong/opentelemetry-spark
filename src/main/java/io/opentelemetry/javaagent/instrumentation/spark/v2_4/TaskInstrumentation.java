package io.opentelemetry.javaagent.instrumentation.spark.v2_4;

import static io.opentelemetry.javaagent.instrumentation.spark.v2_4.ApacheSparkSingletons.OPEN_TELEMETRY;
import static io.opentelemetry.javaagent.instrumentation.spark.v2_4.ApacheSparkSingletons.PROPERTIES_TEXT_MAP_ACCESSOR;
import static net.bytebuddy.matcher.ElementMatchers.*;

import io.opentelemetry.context.Context;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.Properties;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.spark.scheduler.Stage;

public class TaskInstrumentation implements TypeInstrumentation {
  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("org.apache.spark.scheduler.Task");
  }

  @Override
  public void transform(TypeTransformer typeTransformer) {

    typeTransformer.applyAdviceToMethod(
        isConstructor()
            .and(takesArgument(0, Integer.TYPE))
            .and(takesArgument(1, Integer.TYPE))
            .and(takesArgument(2, Integer.TYPE))
            .and(takesArgument(3, Properties.class))
            .and(takesArgument(4, byte[].class))
            .and(takesArgument(5, named("scala.Option")))
            .and(takesArgument(6, named("scala.Option")))
            .and(takesArgument(7, named("scala.Option")))
            .and(takesArgument(8, Boolean.TYPE)),
        this.getClass().getName() + "$Interceptor");
  }

  public static class Interceptor {

    @Advice.OnMethodEnter()
    public static void enter(
        int stageId,
        int stageAttemptId,
        int partition,
        Properties localProperties,
        byte[] serializedTaskMetrics,
        scala.Option jobId,
        scala.Option appId,
        scala.Option appAttemptId,
        boolean isBarrier) {

      // Context stageContext = SparkEventListener.contextByStageId.get(stageId);

      Stage stage = ApacheSparkSingletons.findStage(stageId);
      Context stageContext = ApacheSparkSingletons.getStageContext(stage);

      OPEN_TELEMETRY
          .getPropagators()
          .getTextMapPropagator()
          .inject(stageContext, localProperties, PROPERTIES_TEXT_MAP_ACCESSOR);
    }
  }
}

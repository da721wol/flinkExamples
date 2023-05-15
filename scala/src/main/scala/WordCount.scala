import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.util.Collector

object WordCount {
  def main(args: Array[String]) {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    // set up the streaming execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)
    val text = env.readTextFile("./scala/src/main/resources/Bibel.txt")
    text
      .flatMap((t: String, collector: Collector[Tuple2[String, Integer]]) => {
        t.toLowerCase().split("\\W+").foreach(token => {
          if (token.nonEmpty) {
            collector.collect(new Tuple2(token, 1))
          }
        })
      })
      .returns(TypeInformation.of(new TypeHint[Tuple2[String, Integer]](){}))
      .groupBy(0)
      .sum(1)
      .sortPartition(1, Order.ASCENDING)
      .print()
    env.execute("Scala WordCount Example")
  }
}
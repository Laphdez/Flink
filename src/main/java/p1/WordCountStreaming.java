package p1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class WordCountStreaming {
	public static void main(String[] args) throws Exception {
	    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	    final ParameterTool params = ParameterTool.fromArgs(args);
	    env.getConfig().setGlobalJobParameters(params);
	    DataStream<String> text = env.socketTextStream("localhost", 9999);
	    DataStream<Tuple2<String, Integer>> filtered = text.filter(new FilterFunction<String>() {
	      public boolean filter(String value) {
	        return value.startsWith("N") || value.startsWith("F");	//Devuelve solo las palabras que empiezan con "N" o "F"
	      }
	    }).map(new Tokenizer())						//Separa las lineas en pares (2-tuplas) conteniendo: tupla2: {(nombre, 1)...}
	      .keyBy(value -> value.f0).sum(1);			//Agrupa la tupla por el campo 0 y suma a la tupla el campo 1
	    filtered.print();
	    env.execute("Streaming WordCount");    
  }
		  
  public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
    public Tuple2<String, Integer> map(String value) {
      return new Tuple2(value, Integer.valueOf(1));
    }
  }
}

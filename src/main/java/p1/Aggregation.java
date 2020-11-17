package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {
	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> data = env.readTextFile("/home/flink/Documentos/avg1");
		
		               // month, category,product, profit, 
		DataStream<Tuple4<String, String, String, Integer>> mapped = data.map(new Splitter());      // tuple  [June,Category5,Bat,12]
		                                                                                            //       [June,Category4,Perfume,10,1]
		mapped.keyBy(words -> words.f0).sum(3).writeAsText("/home/flink/Documentos/out1");	// Devuelve la suma
		mapped.keyBy(words -> words.f0).min(3).writeAsText("/home/flink/Documentos/out2");	// Devuelve el mínimo valor		
	   	mapped.keyBy(words -> words.f0).minBy(3).writeAsText("/home/flink/Documentos/out3");// Devuelve el elemento con el mínimo valor	
		mapped.keyBy(words -> words.f0).max(3).writeAsText("/home/flink/Documentos/out4");	// Devuelve el valor máximo	
		mapped.keyBy(words -> words.f0).maxBy(3).writeAsText("/home/flink/Documentos/out5");// Devuelve el elemento con el máximo valor 
		
		env.execute("Aggregation");		// execute program
	}
                                                                                           
	// *************************************************************************
    // USER FUNCTIONS                                                                                      
    // *************************************************************************
		
	public static class Splitter implements MapFunction<String, Tuple4<String, String, String, Integer>> {
		public Tuple4<String, String, String, Integer> map(String value) {        // 01-06-2018,June,Category5,Bat,12
			String[] words = value.split(",");                             // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
			return new Tuple4<String, String, String, Integer>(words[1], words[2],	words[3], Integer.parseInt(words[4])); 
		}                                                  //    June    Category5      Bat               12 
	}
}
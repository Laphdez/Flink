package p1;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * La operación de plegado hace la tarea similar de reducir, es decir, reduce la corrientes, 
 * pero tenemos una pequeña disposición adicional en la operación de plegado. 
 * A diferencia del método de reducción, la entrada y la salida pueden o no ser al mismo tiempo.
 * @author Lara P
 */
@SuppressWarnings("deprecation")
public class FoldOperation {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);	//Chequea los paramentros de entrada
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();	//configura el entorno de ejecución
		
		env.getConfig().setGlobalJobParameters(params);	//hacer que los parámetros estén disponibles en la interfaz web
		
		DataStream<String> data = env.readTextFile("/home/flink/Documentos/avg");
		DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());
		DataStream<Tuple4<String, String, Integer, Integer>> folded = mapped.keyBy(0).fold(new Tuple4<String, String, Integer, Integer>("", "", 0, 0), new FoldFunction1());
		DataStream<Tuple2<String, Double>> beneficioPorMes = folded.map(new MapFunction<Tuple4<String, String, Integer, Integer>, Tuple2<String, Double>>(){
			public Tuple2<String, Double> map(Tuple4<String, String, Integer, Integer> input){
				return new Tuple2<String, Double>(input.f0, new Double((input.f2*1.0)/input.f3));
			}
		});
		//beneficioPorMes.writeAsText("/home/flink/Documentos/fold");
		beneficioPorMes.writeAsText("/home/flink/Documentos/fold");
		env.execute("FOLD");
	}
	
	// *************************************************************************
    // USER FUNCTIONS                             					// pre_result  = Category4,Perfume,22,2        
    // *************************************************************************
	
	public static class FoldFunction1 implements FoldFunction<Tuple5<String, String, String, Integer, Integer>, Tuple4<String, String, Integer, Integer>> {
		public Tuple4<String, String, Integer, Integer> fold(Tuple4<String, String, Integer, Integer> defaultIn,
															Tuple5<String, String, String, Integer, Integer> curr){
			defaultIn.f0 = curr.f0;
			defaultIn.f1 = curr.f1;
			defaultIn.f2 += curr.f3;
			defaultIn.f3 += curr.f4;
			return defaultIn;
		}
	}
	
	public static class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {
		public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current, 
																	   Tuple5<String, String, String, Integer, Integer> pre_result) {
			return new Tuple5<String, String, String, Integer, Integer>(current.f0,current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4); 
		}
	}

	/**
	 * Separa por comas e ignora el timestamp
	 */
	public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
		public Tuple5<String, String, String, Integer, Integer> map(String value) {       // 01-06-2018,June,Category5,Bat,12
			String[] words = value.split(",");                             // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
			// ignora timestamp, no lo necesitamos para los calculos
			return new Tuple5<String, String, String, Integer, Integer>(words[1], words[2],	words[3], Integer.parseInt(words[4]), 1); 
		}                                                            //  June    Category5      Bat                      12 
	}
}

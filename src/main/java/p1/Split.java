package p1;

import java.util.List;
import java.util.ArrayList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SuppressWarnings("deprecation")
public class Split {

     public static void main(String[] args) throws Exception {
	 
         // set up the stream execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	
        // Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
	
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		DataStream<String> text = env.readTextFile("/home/flink/Documentos/oddeven");
	
		SplitStream<Integer> evenOddStream = text.map(new MapFunction<String, Integer>() {
			public Integer map(String value) {
				return Integer.parseInt(value);
			}
		})
		
	   .split(new OutputSelector<Integer>() {
			public Iterable<String> select(Integer value) {
				List<String> out = new ArrayList<String>();
				if (value%2 == 0)
					out.add("par");             
				else
					out.add("impar");
				return out;
			}
	});
	
	DataStream<Integer> evenData = evenOddStream.select("par");
	DataStream<Integer> oddData = evenOddStream.select("impar");
	
	evenData.writeAsText("/home/flink/Documentos/par");
	oddData.writeAsText("/home/flink/Documentos/impar");
	
	// execute program
	env.execute("IMPAR PAR 2");
    }
}
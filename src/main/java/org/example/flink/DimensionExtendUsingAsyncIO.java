package org.example.flink;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.data.Action;
import org.example.flink.data.UserAction;
import org.example.flink.operator.AsyncDatabaseRequest;
import org.example.flink.operator.ColoredPrintSinkFunction;

import com.google.gson.Gson;


public class DimensionExtendUsingAsyncIO {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString("rest.port", "9091");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
		
		// 1. 从Kafka中加载用户行为数据流
		KafkaSource<String> source = KafkaSource.<String>builder()
			.setBootstrapServers("10.4.46.63:9092")
			.setTopics("user-action")
			.setGroupId("group-01")
		    .setStartingOffsets(OffsetsInitializer.latest())
		    .setValueOnlyDeserializer(new SimpleStringSchema())
		    .build();
		
		DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
				"Kafka Source");
		sourceStream.setParallelism(1);	// 设置source算子的并行度为1
		
		// 2. 转换为UserAction对象
		SingleOutputStreamOperator<Action> actionStream = sourceStream
				.map(new MapFunction<String, Action>() {
					
					private static final long serialVersionUID = 1L;
					
					@Override
					public Action map(String value) throws Exception {
						Gson gson = new Gson();
						Action userAction = gson.fromJson(value, Action.class);
						return userAction;
					}
				});
		actionStream.name("Map to Action");
		actionStream.setParallelism(1);	// 设置map算子的并行度为1
		
		// 3. 维表关联
		SingleOutputStreamOperator<UserAction> userActionStream = AsyncDataStream.unorderedWait(actionStream,
				new AsyncDatabaseRequest(), 30, TimeUnit.SECONDS);
		userActionStream.name("Extend Dimensions");
		userActionStream.setParallelism(2);
		
		// 4. 打印关联结果
		//userActionStream.print();
		userActionStream.addSink(new ColoredPrintSinkFunction()).name("Colored Print Result");
		
		// 执行
		env.execute("Dimension Extend Using AsyncIO");
	}
}
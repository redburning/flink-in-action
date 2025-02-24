package org.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.flink.data.Trace;
import org.example.flink.operator.BloomFilterProcessFunction;

import com.google.gson.Gson;


public class StreamDeduplication {

	public static void main(String[] args) throws Exception {
		// 1. prepare
		Configuration configuration = new Configuration();
		configuration.setString("rest.port", "9091");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
		env.enableCheckpointing(2 * 60 * 1000);
		env.setStateBackend(new EmbeddedRocksDBStateBackend());  // 使用rocksDB作为状态后端
		
		// 2. Kafka Source
		KafkaSource<String> source = KafkaSource.<String>builder()
			.setBootstrapServers("10.4.130.17:9092")
			.setTopics("trace-01")
			.setGroupId("group-01")
		    .setStartingOffsets(OffsetsInitializer.latest())
		    .setProperty("commit.offsets.on.checkpoint", "true")
		    .setValueOnlyDeserializer(new SimpleStringSchema())
		    .build();

		DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
				"Kafka Source");
		sourceStream.setParallelism(1);	// 设置source算子的并行度为1
		
		// 3. 转换为Trace对象
		SingleOutputStreamOperator<Trace> mapStream = sourceStream.map(new MapFunction<String, Trace>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Trace map(String value) throws Exception {
				Gson gson = new Gson();
				Trace trace = gson.fromJson(value, Trace.class);
				return trace;
			}
		});
		mapStream.name("Map to Trace");
		mapStream.setParallelism(1);	// 设置map算子的并行度为1
		
		// 4. Bloom过滤器去重, 在去重之前要keyBy处理，保障同一gid的数据全都交由同一个线程处理
		SingleOutputStreamOperator<Trace> deduplicatedStream = mapStream.keyBy(
				new KeySelector<Trace, String>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public String getKey(Trace trace) throws Exception {
						return trace.getGid();
					}
				})
			.process(new BloomFilterProcessFunction());
		deduplicatedStream.name("Bloom filter process for distinct gid");
		deduplicatedStream.setParallelism(2);	// 设置去重算子的并行度为2
		
		// 5. 将去重结果写入DataBase
		DataStreamSink<Trace> sinkStream = deduplicatedStream.addSink(
				JdbcSink.sink("insert into flink.deduplication(gid, timestamp) values (?, ?);",
						(statement, trace) -> {
							statement.setString(1, trace.getGid());
                            statement.setLong(2, trace.getTimestamp());
                        },
						JdbcExecutionOptions.builder()
                        		.withBatchSize(1000)
                        		.withBatchIntervalMs(200)
                        		.withMaxRetries(5)
                        		.build(), 
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            	.withUrl("jdbc:mysql://127.0.0.1:3306/flink")
                            	.withUsername("root")
                            	.withPassword("aaa123+-*/")
                            	.build())
				);
		sinkStream.name("Sink DB");
		sinkStream.setParallelism(1);
		
		// 执行
		env.execute("Real-Time DateHistogram");
	}
}
package org.example.flink;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.example.flink.data.BucketCount;

import com.alibaba.fastjson.JSONObject;


public class RealTimeDateHistogram {

	public static void main(String[] args) throws Exception {
		// 1. prepare
		Configuration configuration = new Configuration();
		configuration.setString("rest.port", "9091");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
		env.enableCheckpointing(2 * 60 * 1000);
		// 使用rocksDB作为状态后端
		env.setStateBackend(new EmbeddedRocksDBStateBackend());
		
		// 2. Kafka Source
		KafkaSource<String> source = KafkaSource.<String>builder()
			.setBootstrapServers("10.4.79.90:9092")
			.setTopics("trace-2024")
			.setGroupId("group-01")
		    .setStartingOffsets(OffsetsInitializer.latest())
		    .setProperty("commit.offsets.on.checkpoint", "true")
		    .setValueOnlyDeserializer(new SimpleStringSchema())
		    .build();

		DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
				"Kafka Source");
		sourceStream.setParallelism(2);	// 设置source算子的并行度为2
		
		// 3. 转换为易于统计的BucketCount对象结构{ bucket: 00:00, count: 200 }
		SingleOutputStreamOperator<BucketCount> mapStream = sourceStream
			.map(new MapFunction<String, BucketCount>() {
				
				private static final long serialVersionUID = 1L;
				
				@Override
				public BucketCount map(String value) throws Exception {
					JSONObject jsonObject = JSONObject.parseObject(value);
					long timestamp = jsonObject.getLongValue("timestamp");
					return new BucketCount(timestamp);
				}
			});
		mapStream.name("Map to BucketCount");
		mapStream.setParallelism(2);	// 设置map算子的并行度为2
		
		// 4. 设置eventTime字段作为watermark，要考虑数据乱序到达的情况
		SingleOutputStreamOperator<BucketCount> mapStreamWithWatermark = mapStream
			.assignTimestampsAndWatermarks(
			    WatermarkStrategy.<BucketCount>forBoundedOutOfOrderness(Duration.ofSeconds(60))
					.withIdleness(Duration.ofSeconds(60))
					.withTimestampAssigner(new SerializableTimestampAssigner<BucketCount>() {
						
						private static final long serialVersionUID = 1L;
						
						@Override
						public long extractTimestamp(BucketCount bucketCount, long recordTimestamp) {
							// 提取eventTime字段作为watermark
							return bucketCount.getTimestamp();
						}
					}));
		mapStreamWithWatermark.name("Assign EventTime as Watermark");
		
		// 5. 滚动时间窗口聚合
		SingleOutputStreamOperator<BucketCount> windowReducedStream = mapStreamWithWatermark
			.windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))	// 滚动时间窗口
			.trigger(ProcessingTimeTrigger.create()) // ProcessingTime触发器
			.allowedLateness(Time.seconds(120))  	 // 数据延迟容忍度, 允许数据延迟乱序到达
			.reduce(new ReduceFunction<BucketCount>() {
				
				private static final long serialVersionUID = 1L;
				
				@Override
				public BucketCount reduce(BucketCount bucket1, BucketCount bucket2) throws Exception {
					// 将两个bucket合并，count相加
					return new BucketCount(bucket1.getBucket(), bucket1.getCount() + bucket2.getCount());
				}
			});
		windowReducedStream.name("Window Reduce");
		windowReducedStream.setParallelism(1);		// reduce算子的并行度只能是1
		
		// 6. 将结果写入到数据库
		DataStreamSink<BucketCount> sinkStream = windowReducedStream.addSink(
				JdbcSink.sink("insert into flink.datehistogram(bucket, count) values (?, ?) "
						+ "on duplicate key update count = VALUES(count);",
						(statement, bucketCount) -> {
							statement.setString(1, bucketCount.getBucket());
                            statement.setLong(2, bucketCount.getCount());
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
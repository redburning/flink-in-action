package org.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.flink.data.Trace;

import com.google.gson.Gson;


public class FromKafkaToEs {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString("rest.port", "9091");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
		// 一定要开启checkpoint，只有开启了checkpoint才能保证一致性语义
		env.enableCheckpointing(2 * 60 * 1000);
		// 使用rocksDB作为状态后端
		env.setStateBackend(new EmbeddedRocksDBStateBackend());
		
		// 1. Kafka Source
		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers("10.4.79.90:9092")
				.setTopics("trace-2024")
				.setGroupId("group-01")
			    .setStartingOffsets(OffsetsInitializer.committedOffsets())
			    .setProperty("commit.offsets.on.checkpoint", "true")
			    .setValueOnlyDeserializer(new SimpleStringSchema())
			    .build();

		DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
				"Kafka Source");
		sourceStream.setParallelism(2);	// 设置source算子的并行度为2
		
		// 2. Map转换为Trace对象
		SingleOutputStreamOperator<Trace> mapStream = sourceStream.map(new MapFunction<String, Trace>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Trace map(String value) throws Exception {
				Gson gson = new Gson();
				Trace trace = gson.fromJson(value, Trace.class);
				return trace;
			}
			
		});
		mapStream.setParallelism(1);	// 设置map算子的并行度为1
		
		// 3. 写入es
		DataStreamSink<Trace> sinkStream = mapStream.sinkTo(
			    new Elasticsearch7SinkBuilder<Trace>()
			        .setHosts(new HttpHost("10.4.53.104", 9200, "http"))
			        .setEmitter(
			        	(element, context, indexer) -> {
		        			IndexRequest indexRequest = Requests.indexRequest()
			        				.index("trace-index")
			        				.source(element.toString(), XContentType.JSON);
							indexer.add(indexRequest);
			        	}
			        )
			        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
			        // This enables an exponential backoff retry mechanism, 
			        // with a maximum of 5 retries and an initial delay of 1000 milliseconds
			        .setBulkFlushBackoffStrategy(FlushBackoffType.CONSTANT, 5, 1000)
			        .build());
		sinkStream.setParallelism(2);	// 设置sink算子的并行度为2		
		
		// 执行
		env.execute("From-Kafka-To-ElasticSearch");
	}

}

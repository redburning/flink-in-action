package org.example.test.mysql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;


public class MySqlBinlog {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		
		MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("flink")
                .tableList("flink.user")
                .username("root")
                .password("aaa123+-*/")
                .startupOptions(StartupOptions.initial())	// 设置initial才会加载存量数据
                .deserializer(new JsonDebeziumDeserializationSchema())	// 读取到的binlog反序列化为json格式
                .build();
		
		DataStreamSource<String> sourceStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
		sourceStream.setParallelism(1).print();
		
        env.execute("Print MySQL snapshot + Binlog");

	}

}

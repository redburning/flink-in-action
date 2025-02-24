package org.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.flink.data.Action;
import org.example.flink.data.User;
import org.example.flink.data.UserAction;
import org.example.flink.operator.ColoredPrintSinkFunction;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;


public class DimensionExtendUsingBroadcast {

	private static final String INSERT = "+I";
	private static final String UPDATE = "+U";
	private static final String DELETE = "-D";
	
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
		
		// 3. 读取MySQL维表的binlog
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
		
		DataStream<String> userBinlogStream = env
				.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
				.setParallelism(1);		// 设置MySQL维表读取并行度为1
		
		// 4. 将json格式的binlog转换为User对象
		DataStream<User> userStream = userBinlogStream
				.map(new MapFunction<String, User>() {
					
					private static final long serialVersionUID = 1L;
					
					@Override
					public User map(String value) throws Exception {
						JSONObject jsonObject = JSONObject.parseObject(value);
						JSONObject before = jsonObject.getJSONObject("before");	
						JSONObject after = jsonObject.getJSONObject("after");
						// binlog日志before和after都不为null, 表示为更新操作, 用'+U'标记
						if (before != null && after != null) {
							return new User(UPDATE, after.toJSONString());
						} 
						// binlog日志before为null, 表示为插入操作, 用'+I'标记
						else if (before == null) {
							return new User(INSERT, after.toJSONString());
						}
						// binlog日志after为null, 表示为删除操作, 用'-D'标记
						else {
							return new User(DELETE, before.toJSONString());
						}
					}
				})
				.setParallelism(1);
		
		// 5. 将User维表转换为broadcast state
		MapStateDescriptor<Integer, User> broadcastStateDescriptor = new MapStateDescriptor<>(
                "user-state",
                Integer.class,
                User.class
        );
		BroadcastStream<User> broadcastUserStream = userStream.broadcast(broadcastStateDescriptor);
		
		// 6. 将维表信息传递(connect)给流表, 即维表与流表关联
		DataStream<UserAction> userActionStream = actionStream.connect(broadcastUserStream)
				.process(new BroadcastProcessFunction<Action, User, UserAction>() {
					
					private static final long serialVersionUID = 1L;
					
					@Override
					public void processElement(Action action, ReadOnlyContext ctx, Collector<UserAction> out)
							throws Exception {
						UserAction userAction = new UserAction(action);
						
						// 从broadcast state中获取维表信息
						BroadcastState<Integer, User> broadcastState = (BroadcastState<Integer, User>) ctx
								.getBroadcastState(broadcastStateDescriptor);
						
						User user = broadcastState.get(action.getUserId());
						// 流表与维表关联
						if (user != null) {
							userAction.setUser(user);
						}
						out.collect(userAction);
					}
					
					@Override
					public void processBroadcastElement(User user, Context ctx, Collector<UserAction> out)
							throws Exception {
						
						// 维表更新时, 更新broadcast state
						BroadcastState<Integer, User> broadcastState = (BroadcastState<Integer, User>) ctx
								.getBroadcastState(broadcastStateDescriptor);
						
						if (user.getOp() == null || user.getOp().equals(UPDATE) || user.getOp().equals(INSERT)) {
							// 新增/更新数时，更新broadcast state
							broadcastState.put(user.getId(), user);
						} else if (user.getOp().equals(DELETE)) {
							// 删除数时，更新broadcast state
							broadcastState.remove(user.getId());
						}
					}
				})
				.setParallelism(1);
		
		// 7. 打印关联结果
		//userActionStream.print();
		userActionStream.addSink(new ColoredPrintSinkFunction()).name("Colored Print Result");
		
		// 执行
		env.execute("Dimension Extend Using Broadcast");
	}
}
package org.example.flink.operator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.example.flink.data.Action;
import org.example.flink.data.User;
import org.example.flink.data.UserAction;

public class AsyncDatabaseRequest extends RichAsyncFunction<Action, UserAction> {

	private static final long serialVersionUID = 1L;

	private transient Connection connection;
	private transient PreparedStatement statement;
	
	@Override
    public void open(Configuration configuration) throws Exception {
		String url = "jdbc:mysql://127.0.0.1:3306/flink";
		String username = "root";
		String password = "aaa123+-*/";
		connection = DriverManager.getConnection(url, username, password);
		statement = connection.prepareStatement("SELECT name, age, email FROM user WHERE ID = ?");
	}
	
	@Override
	public void asyncInvoke(Action action, ResultFuture<UserAction> resultFuture) throws Exception {
		CompletableFuture.supplyAsync(new Supplier<User>() {

            @Override
            public User get() {
            	// 获取实时流的id
            	int id = action.getUserId();
            	// 根据实时流的id去获取维表数据
            	try {
					statement.setInt(1, id);
					ResultSet rs = statement.executeQuery();
	        		User user = new User();
	        		while (rs.next()) {
	        			String name = rs.getString("name");
	        			int age = rs.getInt("age");
	        			String email = rs.getString("email");
	        			user = new User(id, name, age, email);
	        		}
	        		return user;
				} catch (SQLException e) {
					e.printStackTrace();
					return null;
				}
            }
        }).thenAccept( (User user) -> {
        	if (user != null) {
        		resultFuture.complete(Collections.singleton(new UserAction(user, action)));
        	} else {
        		System.out.println("User not found for action: " + action.toString());
        		resultFuture.complete(Collections.emptyList());
        	}
        });
	}

	@Override
    public void close() throws Exception {
		// 关闭连接
		if (statement != null) {
			statement.close();
		}
		if (connection != null) {
			connection.close();
		}
	}
}

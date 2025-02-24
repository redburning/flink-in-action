package org.example.flink.operator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.example.flink.data.User;
import org.example.flink.data.Action;
import org.example.flink.data.UserAction;

public class DimExtendRichMapFunction extends RichMapFunction<Action, UserAction> {

	private static final long serialVersionUID = 1L;

	private transient Connection connection;
	
	// user数据缓存
	private Map<Integer, User> userInfoMap;
	
	@Override
    public void open(Configuration configuration) throws Exception {
		String url = "jdbc:mysql://127.0.0.1:3306/flink";
		String username = "root";
		String password = "aaa123+-*/";
		connection = DriverManager.getConnection(url, username, password);
		// 加载维表数据到缓存中
		userInfoMap = new HashMap<>();
		PreparedStatement ps = connection.prepareStatement("SELECT id, name, age, email FROM user");
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			String email = rs.getString("email");
			User user = new User(id, name, age, email);
			userInfoMap.put(id, user);
		}
	}
	
	/**
	 * 流表数据与维表数据关联
	 */
	@Override
	public UserAction map(Action action) throws Exception {
		UserAction userAction = new UserAction(action);
		int userId = action.getUserId();
		if (userInfoMap.containsKey(userId)) {
			User user = userInfoMap.get(userId);
			userAction.setUser(user);
		}
		return userAction;
	}
	
	@Override
    public void close() throws Exception {
		// 关闭数据库连接
		if (connection != null) {
			connection.close();
		}
	}
}

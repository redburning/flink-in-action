package org.example.flink.data;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class User {

	private int id;
	
	private String name;
	
	private int age;
	
	private String email;
	
	// 标记对用户的操作：插入(+I), 更新(+U), 删除(-D)
	private String op;

	public User() {}
	
	public User(String op, String jsonFormatUser) {
		User user = (new Gson()).fromJson(jsonFormatUser, User.class);
		this.op = op;
		this.id = user.id;
		this.name = user.name;
		this.age = user.age;
		this.email = user.email;
	}
	
	public User(int id, String name, int age, String email) {
		this.id = id;
		this.name = name;
		this.age = age;
		this.email = email;
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}
	
	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
	}
	
	@Override
	public String toString() {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		return gson.toJson(this, User.class);
	}

}

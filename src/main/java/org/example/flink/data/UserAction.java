package org.example.flink.data;

import com.google.gson.Gson;

public class UserAction {

	private User user;
	
	private int productId;
	
	private String eventType;
	
	private long timestamp;

	public UserAction(User user, Action action) {
		this(action);
		this.user = user;
	}
	
	public UserAction(Action action) {
		this.productId = action.getProductId();
		this.eventType = action.getEventType();
		this.timestamp = action.getTimestamp();
	}
	
	public User getUser() {
		return user;
	}

	public void setUser(User user) {
		this.user = user;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	@Override
	public String toString() {
		Gson gson = new Gson();
		return gson.toJson(this, UserAction.class);
	}
	
}

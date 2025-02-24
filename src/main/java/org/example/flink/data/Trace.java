package org.example.flink.data;

import com.google.gson.Gson;

public class Trace {

	private String gid;
	private long timestamp;
	
	public String getGid() {
		return gid;
	}
	
	public void setGid(String gid) {
		this.gid = gid;
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
		return gson.toJson(this, Trace.class);
	}
}

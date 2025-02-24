package org.example.flink.data;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class BucketCount {

	private long timestamp;
	private String bucket;
	private long count;

	public BucketCount(long timestamp) {
		this.timestamp = timestamp;
		this.bucket = formatTimeInterval(timestamp);
		this.count = 1;
	}
	
	public BucketCount(String bucket, long count) {
		this.bucket = bucket;
		this.count = count;
	}
	
	/**
	 * 将时间戳格式化为时间区间格式
	 * 
	 * @param time
	 * @return 例如 [11:28:00 — 11:28:05]
	 */
	private String formatTimeInterval(long time) {
        // 定义输出的日期时间格式
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

        // 将时间戳转换为 LocalDateTime 对象
        LocalDateTime dateTime = Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();

        // 提取秒数并计算区间开始时间
        int seconds = dateTime.getSecond();
        int intervalStartSeconds = (seconds / 5) * 5;

        // 创建区间开始和结束时间的 LocalDateTime 对象
        LocalDateTime intervalStartTime = dateTime.withSecond(intervalStartSeconds);
        LocalDateTime intervalEndTime = intervalStartTime.plusSeconds(5);

        // 格式化区间开始和结束时间为字符串
        String startTimeString = intervalStartTime.format(outputFormatter);
        String endTimeString = intervalEndTime.format(outputFormatter);

        // 返回格式化后的时间区间字符串
        return startTimeString + "-" + endTimeString;
    }
	
	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public String getBucket() {
		return bucket;
	}

	public void setBucket(String bucket) {
		this.bucket = bucket;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
	
}

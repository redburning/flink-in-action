package org.example.test.kafka;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.alibaba.fastjson.JSONObject;

public class KafkaDataProducer {

	private static final String TEST_DATA = "testdata.json";
	private static final String GID = "gid";
	private static final String TIMESTAMP = "timestamp";
	
	// 定义颜色
	public static final String reset = "\u001B[0m";
	public static final String red = "\u001B[31m";
	public static final String green = "\u001B[32m";
	
	private static int charToInt(char c) {
		return Integer.valueOf(String.valueOf(c));
	}
	
	private static String increase(String number1, String number2) {
		String res = "";
		int extra = 0;
		int i = number1.length() - 1;
		int j = number2.length() - 1;
		while (i >= 0 && j >= 0) {
			int v = (charToInt(number1.charAt(i)) + charToInt(number2.charAt(j)) + extra) % 10;
			extra = (charToInt(number1.charAt(i)) + charToInt(number2.charAt(j)) + extra) / 10;
			res = String.valueOf(v) + res;
			i--;
			j--;
		}
		while (i >= 0) {
			int v = (charToInt(number1.charAt(i)) + extra) % 10;
			extra = (charToInt(number1.charAt(i)) + extra) / 10;
			res = String.valueOf(v) + res;
			i--;
		}
		while (j >= 0) {
			int v = (charToInt(number2.charAt(j)) + extra) % 10;
			extra = (charToInt(number2.charAt(j)) + extra) / 10;
			res = String.valueOf(v) + res;
			j--;
		}
		if (extra != 0) {
			res = String.valueOf(extra) + res;
		}
		return res;
	}
	
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		String topic = "trace-01";
		props.put("bootstrap.servers", "10.4.130.17:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		InputStream inputStream = KafkaDataProducer.class.getClassLoader().getResourceAsStream(TEST_DATA);
		Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name());
        String content = scanner.useDelimiter("\\A").next();
        scanner.close();
		JSONObject jsonContent = JSONObject.parseObject(content);
		
		int nonDuplicateNum = 10;
		int repeatNum = 10;
		Random r = new Random();
		for (int i = 0; i < nonDuplicateNum; i++) {
			String id = jsonContent.getString(GID);
			String newId = increase(id, String.valueOf(i));
			jsonContent.put(GID, newId);
			// 制造重复数据
			for (int j = 0; j < repeatNum; j++) {
				// 对时间进行随机扰动，模拟数据乱序到达
				long current = System.currentTimeMillis() - r.nextInt(60) * 1000;
				jsonContent.put(TIMESTAMP, current);
				producer.send(new ProducerRecord<String, String>(topic, jsonContent.toString()));
			}
			// wait some time
			Thread.sleep(2 * r.nextInt(10));
			System.out.print("\r" + "Send " + green + (i + 1) * repeatNum + reset + " records to Kafka, Non-repeated record number: " + red + (i + 1) + reset);
		}
		Thread.sleep(2000);
		System.out.println("\n");
		System.out.println("finished");
		producer.close();
	}
	
}
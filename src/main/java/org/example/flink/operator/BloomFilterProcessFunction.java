package org.example.flink.operator;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.flink.data.Trace;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;


public class BloomFilterProcessFunction extends KeyedProcessFunction<String, Trace, Trace> {

	private static final long serialVersionUID = 1L;
	
	// bloom预计插入的数据量
	private static final long EXPECTED_INSERTIONS = 5000000L;
	// bloom的假阳性率
	private static final double FPP = 0.001;
	// bloom过滤器TTL
	private static final long TTL = 60 * 1000;
	// bloom过滤器队列size
	private static final int FILTER_QUEUE_SIZE = 10;
	// bloom过滤器队列
	private List<BloomFilter<String>> bloomFilterList;
	// 是否已经注册定时器
	private boolean registeredTimerTask = false;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		bloomFilterList = new ArrayList<>(FILTER_QUEUE_SIZE);
		BloomFilter<String> bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.forName("utf-8")),
				EXPECTED_INSERTIONS, FPP);
		bloomFilterList.add(bloomFilter);
	}
	
	@Override
	public void processElement(Trace trace, KeyedProcessFunction<String, Trace, Trace>.Context context,
			Collector<Trace> out) throws Exception {
		BloomFilter<String> firstBloomFilter = bloomFilterList.get(0);
		String key = trace.getGid();
		// 只要有一个bloom未hit该元素，就意味着该元素从未出现过，在队列中的所有过滤器留下该元素的标记
		if (!firstBloomFilter.mightContain(key)) {
			for (BloomFilter<String> bloomFilter : bloomFilterList) {
				bloomFilter.put(key);
			}
			// 该元素从未出现过，为非重复数据
			out.collect(trace);
		}
		if (!registeredTimerTask) {
			long current = context.timerService().currentProcessingTime();
			context.timerService().registerProcessingTimeTimer(current + TTL);
			registeredTimerTask = true;
		}
	}
	
	@Override
	public void onTimer(long timestamp, OnTimerContext context, Collector<Trace> out) throws Exception {
		// append新的bloomFilter到bloom过滤器队列
		bloomFilterList
				.add(BloomFilter.create(Funnels.stringFunnel(Charset.forName("utf-8")), EXPECTED_INSERTIONS, FPP));
		// 清理第一个bloomFilter
		if (bloomFilterList.size() > FILTER_QUEUE_SIZE) {
			bloomFilterList.remove(0);
		}
		// 创建一个新的timer task
		context.timerService().registerProcessingTimeTimer(timestamp + TTL);
	}

	@Override
	public void close() throws Exception {
		bloomFilterList = null;
	}

}

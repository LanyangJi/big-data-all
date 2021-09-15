package cn.jly.bigdata.project.statistical_analysis;

import cn.jly.bigdata.project.beans.ApacheLogEvent;
import cn.jly.bigdata.project.beans.PageViewCount;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * 热门页面统计
 * <p>
 * 统计每天热门页面前三名
 *
 * @author jilanyang
 * @date 2021/9/4 10:11
 */
public class D04_HotPages {

	private static final String METHOD = "get";

	@SneakyThrows
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
		env.setParallelism(1);

		// file source
		DataStreamSource<String> lineDS = env.readTextFile("./input/apache.log");

		SingleOutputStreamOperator<ApacheLogEvent> logEventDS = lineDS.flatMap(new FlatMapFunction<String, ApacheLogEvent>() {
			private final SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
			/**
			 * 对象循环利用
			 */
			private final ApacheLogEvent apacheLogEvent = new ApacheLogEvent();

			@Override
			public void flatMap(String value, Collector<ApacheLogEvent> out) throws Exception {
				String[] fields = value.split(" ");
				long timestamp = dateFormat.parse(fields[3]).getTime();

				apacheLogEvent.setIp(fields[0]);
				apacheLogEvent.setUserId(fields[1]);
				apacheLogEvent.setTimestamp(timestamp);
				apacheLogEvent.setMethod(fields[5]);
				apacheLogEvent.setUrl(fields[6]);

				out.collect(apacheLogEvent);
			}
		}).assignTimestampsAndWatermarks(
				// 分配水印和指定时间戳，允许最大1分钟的乱序和延迟
				WatermarkStrategy.<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofMinutes(10))
						.withTimestampAssigner(new SerializableTimestampAssigner<ApacheLogEvent>() {
							@Override
							public long extractTimestamp(ApacheLogEvent element, long recordTimestamp) {
								return element.getTimestamp();
							}
						}));

		// 分组开窗聚合
		SingleOutputStreamOperator<PageViewCount> windowAggDS = logEventDS.filter(new FilterFunction<ApacheLogEvent>() {
					@Override
					public boolean filter(ApacheLogEvent event) throws Exception {
						return METHOD.equalsIgnoreCase(event.getMethod());
					}
				}).keyBy(new KeySelector<ApacheLogEvent, String>() {
					@Override
					public String getKey(ApacheLogEvent value) throws Exception {
						return value.getUrl();
					}
				}).window(TumblingEventTimeWindows.of(Time.days(1)))
				// WindowProcessFunction需要等窗口集齐了才触发，内存中存储着整个窗口的数据，有一定的内存损耗；
				//    ReduceFunction和AggregateFunction会每来一条数据就会聚合，最终窗口中只存了一条聚合结果
				.aggregate(
						// 聚合函数
						new AggregateFunction<ApacheLogEvent, Long, Long>() {
							@Override
							public Long createAccumulator() {
								return 0L;
							}

							@Override
							public Long add(ApacheLogEvent value, Long accumulator) {
								return accumulator + 1;
							}

							@Override
							public Long getResult(Long accumulator) {
								return accumulator;
							}

							@Override
							public Long merge(Long a, Long b) {
								return a + b;
							}
						},

						// 全窗口函数
						new WindowFunction<Long, PageViewCount, String, TimeWindow>() {
							private final PageViewCount pageViewCount = new PageViewCount();

							@Override
							public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out)
									throws Exception {
								pageViewCount.setCount(input.iterator().next());
								pageViewCount.setUrl(key);
								pageViewCount.setWindowEnd(window.getEnd());

								out.collect(pageViewCount);
							}
						});

		// 按照窗口排序，并输出前三名
		windowAggDS.keyBy(new KeySelector<PageViewCount, Long>() {
			@Override
			public Long getKey(PageViewCount value) throws Exception {
				return value.getWindowEnd();
			}
		}).process(new KeyedProcessFunction<Long, PageViewCount, PageViewCount>() {

			// 定义一个状态，存储当前PageViewCount
			private ListState<PageViewCount> listState = null;

			@Override
			public void open(Configuration parameters) throws Exception {
				this.listState = getRuntimeContext().getListState(
						new ListStateDescriptor<PageViewCount>("PageViewCount", PageViewCount.class));
			}

			@Override
			public void processElement(PageViewCount pageViewCount, KeyedProcessFunction<Long, PageViewCount, PageViewCount>.Context ctx,
					Collector<PageViewCount> out) throws Exception {
				this.listState.add(pageViewCount);

				ctx.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
			}

			@Override
			public void onTimer(long timestamp, KeyedProcessFunction<Long, PageViewCount, PageViewCount>.OnTimerContext ctx,
					Collector<PageViewCount> out) throws Exception {
				ArrayList<PageViewCount> list = Lists.newArrayList(this.listState.get());
				list.sort(new Comparator<PageViewCount>() {
					@Override
					public int compare(PageViewCount o1, PageViewCount o2) {
						return -Long.compare(o1.getCount(), o2.getCount());
					}
				});

				for (int i = 0; i < Math.min(3, list.size()); i++) {
					out.collect(list.get(i));
				}
			}
		}).map(new MapFunction<PageViewCount, String>() {
			final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			@Override
			public String map(PageViewCount value) throws Exception {
				StringBuilder sb = new StringBuilder();
				sb.append("窗口结束时间：").append(dateFormat.format(value.getWindowEnd())).append(",").append("页面：").append(value.getUrl())
						.append(",").append("页面访问数量：").append(value.getCount());
				return sb.toString();
			}
		}).print();

		env.execute("D04_HotPages");
	}
}

package io.github.spafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class FlinkkafkaWc {

	public static void main(String[] args) throws Exception {


		Configuration conf = new Configuration();
		conf.setInteger(RestOptions.PORT, 8080);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		properties.putIfAbsent("group.id", "flink-test");
		properties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		// 60s 发现一次
		properties.put("flink.partition-discovery.interval-millis", "60000");
		// 10s提交一次offset
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");


		// 使用正则表达式，匹配所有topic，后台线程定时刷新，获取新的topic
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer(
			"flink", new KafkaDeserializationSchema<String>() {
			@Override
			public boolean isEndOfStream(String nextElement) {
				return false;
			}

			@Override
			public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
				return new String(record.value());
			}

			@Override
			public TypeInformation getProducedType() {
				return BasicTypeInfo.STRING_TYPE_INFO;
			}
		}, properties);

		//consumer.setCommitOffsetsOnCheckpoints(true);

		consumer.setStartFromEarliest();

		SingleOutputStreamOperator<WordWithCount> windowCounts = env
			.addSource(consumer)
			.setParallelism(1)
			.flatMap(new FlatMapFunction<String, WordWithCount>() {
				@Override
				public void flatMap(String value, Collector<WordWithCount> out) {
					for (String word : value.split("\\s")) {
						out.collect(new WordWithCount(word, 1L));
					}
				}
			})

			.keyBy("word")
			//.timeWindow(Time.seconds(5))

			.reduce(new ReduceFunction<WordWithCount>() {
				@Override
				public WordWithCount reduce(WordWithCount a, WordWithCount b) {
					return new WordWithCount(a.word, a.count + b.count);
				}
			});


		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);

		env.execute("Socket Window WordCount");
	}

	// ------------------------------------------------------------------------
	/**
	 * Data type for words with count.
	 */
	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {
		}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}

}


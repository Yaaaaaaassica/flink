/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.spafka;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

import static io.github.spafka.Utils.getStreamEnv;


@SuppressWarnings("serial")
public class KafkaWindowWordCount {

	public static void main(String[] args) throws Exception {

		// the host and the port to connect to
		final String topic;
		final String hostname;
		final int port;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			topic= params.has("topic") ? params.get("topic") : "test";
			hostname = params.has("hostname") ? params.get("hostname") : "localhost";
			port = params.getInt("port",9092);
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'SocketWindowWordCount " +
				"--hostname <hostname> --port <port>', where hostname (localhost by default) " +
				"and port is the address of the text server");
			System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
				"type the input text into the command line");
			return;
		}

		// get the execution environment
		final StreamExecutionEnvironment env = getStreamEnv();

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


		Properties props = new Properties();
		props.setProperty("bootstrap.servers", hostname+":"+port);
		props.setProperty("group.id", "flink");

		FlinkKafkaConsumer<String> consumer =
			new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
		//consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

		DataStream<Tuple2<String, Long>> keyedStream = env
			.addSource(consumer)
			.flatMap(new MessageSplitter())
			.keyBy(0)
			.timeWindow(Time.seconds(10))
			.apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple, TimeWindow>() {
				@Override
				public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {

					final long[] count = {0};
					input.iterator().forEachRemaining(x->{
						count[0] +=x.f1;});
					out.collect(new Tuple2<String, Long>(tuple.getFieldNotNull(0),count[0]));
				}
			});

		keyedStream.print();
		 env.execute("Flink-Kafka demo");
	}
}

class MessageSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {

	@Override
	public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
		if (value != null ) {
			String[] parts = value.split(" ");

			for (String part : parts) {
				out.collect(new Tuple2(part, 1L));
			}

		}
	}
}

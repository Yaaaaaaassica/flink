/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.spafka;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An example of grouped stream windowing into sliding time windows.
 * This example uses [[RichParallelSourceFunction]] to generate a list of key-value pairs.
 */
public class GroupedProcessingTimeWindowExample{

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		conf.setInteger(RestOptions.PORT, 8080);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		env.setParallelism(4);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


		env.disableOperatorChaining();

		DataStream<Tuple2<Long, Long>> stream = env.addSource(new DataSource());

  			stream
			.keyBy(0)
			.timeWindow(Time.of(2500, MILLISECONDS), Time.of(500, MILLISECONDS))
				.evictor(new Evictor<Tuple2<Long, Long>, TimeWindow>() {
					@Override
					public void evictBefore(Iterable<TimestampedValue<Tuple2<Long, Long>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

					}

					@Override
					public void evictAfter(Iterable<TimestampedValue<Tuple2<Long, Long>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

					}
				})
			.reduce(new SummingReducer())
			// alternative: use a apply function which does not pre-aggregate
//			.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
//				@Override
//				public Long getKey(Tuple2<Long, Long> value) throws Exception {
//					return value.f0;
//				}
//			})
//			.timeWindow(Time.of(2500, MILLISECONDS), Time.of(500, MILLISECONDS))
//			.apply(new SummingWindowFunction())

			.addSink(new SinkFunction<Tuple2<Long, Long>>() {
				@Override
				public void invoke(Tuple2<Long, Long> value) {
					System.out.println(value);
				}
			});

		env.execute();
	}

	private static class FirstFieldKeyExtractor<Type extends Tuple, Key> implements KeySelector<Type, Key> {

		@Override
		@SuppressWarnings("unchecked")
		public Key getKey(Type value) {
			return (Key) value.getField(0);
		}
	}

	private static class SummingWindowFunction implements WindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long, TimeWindow> {

		@Override
		public void apply(Long key, TimeWindow window, Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) {
			long sum = 0L;
			for (Tuple2<Long, Long> value : values) {
				sum += value.f1;
			}

			out.collect(new Tuple2<>(key, sum));
		}
	}

	private static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
			return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
		}
	}

	/**
	 * Parallel data source that serves a list of key-value pairs.
	 */
	private static class DataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

			final long startTime = System.currentTimeMillis();

			final long numElements = 2000000;
			final long numKeys = 1000;
			long val = 1L;
			long count = 0L;

			while (running && count < numElements) {
				count++;
				ctx.collect(new Tuple2<>(val++, 1L));

				if (val > numKeys) {
					val = 1L;
				}
			}

			final long endTime = System.currentTimeMillis();
			System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}

package io.github.spafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;


public class GroupedProcessingTimeWindowExample {

    public static void main(String[] args) throws Exception {


        long start = System.currentTimeMillis();
        final StreamExecutionEnvironment env = Utils.getStreamEnv();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.disableOperatorChaining();


        DataStream<Tuple3<Long, Long,Long>> stream = env.addSource(new DataSource());

		final OutputTag<Tuple2<Long,Long>> lateOutputTag = new OutputTag<Tuple2<Long,Long>>("late-data"){};

		SingleOutputStreamOperator<Tuple2<Long, Long>> reduce = stream
			// 每次都会触发water的发送，对下游的计算有压力
//                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple3<Long, Long, Long>>() {
//					@Nullable
//					@Override
//					public Watermark checkAndGetNextWatermark(Tuple3<Long, Long, Long> lastElement, long extractedTimestamp) {
//						return new Watermark(extractedTimestamp);
//					}
//
//					@Override
//					public long extractTimestamp(Tuple3<Long, Long, Long> element, long previousElementTimestamp) {
//						return element.f2;
//					}
//				})

			// 默认没200ms，计算一次当前的watermark，然后向下游发送
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, Long, Long>>(Time.of(0, SECONDS)) {
				@Override
				public long extractTimestamp(Tuple3<Long, Long, Long> element) {
					return element.f2;
				}
			})
			.map(new RichMapFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>>() {
				@Override
				public Tuple2<Long, Long> map(Tuple3<Long, Long, Long> value) throws Exception {
					return new Tuple2<Long, Long>(value.f0, value.f1);
				}
			})
			.keyBy(0)
			.timeWindow(Time.of(5, SECONDS))
			.allowedLateness(Time.of(1, SECONDS))
			.sideOutputLateData(lateOutputTag)
			//.process( new ProcessWindowFunction1())
			.reduce(new SummingReducer(), new PassThroughWindowChechPointFunction());
		reduce
                .addSink(new SinkFunction<Tuple2<Long, Long>>() {
                    @Override
                    public void invoke(Tuple2<Long, Long> value) {
                    	System.out.println("当前窗口，" + value);
                    }
                });

		DataStream<Tuple2<Long,Long>> lateStream = reduce.getSideOutput(lateOutputTag);

		lateStream.print();

        env.execute();

        System.out.println(System.currentTimeMillis() - start);
    }


    private static class ProcessWindowFunction1 extends ProcessWindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple, TimeWindow> implements CheckpointedFunction {


		private ReducingState<Tuple2<Long, Long>> countPerKey;

		private Tuple2<Long, Long> localCount;

		@Override
		public void process(Tuple tuple, Context context, Iterable<Tuple2<Long, Long>> elements, Collector<Tuple2<Long, Long>> out) throws Exception {
			for (Tuple2<Long, Long> x : elements) {
				countPerKey.add(x);
				//out.collect(x);
			}
			System.err.println("now "+new Date() +", TimeWindow ="+context.window()+",key = " + tuple + "value = " + countPerKey.get());

		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {

		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			countPerKey = context.getKeyedStateStore().getReducingState(
				new ReducingStateDescriptor<Tuple2<Long, Long>>("perKeyCount", new ReduceFunction<Tuple2<Long, Long>>() {
					@Override
					public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
						return Tuple2.of(value1.f0, value1.f1 + value2.f1);
					}
				}, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
				})));
		}
	}

    @Slf4j
    private static class PassThroughWindowChechPointFunction extends RichWindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple, TimeWindow> implements CheckpointedFunction {

        private ReducingState<Tuple2<Long, Long>> countPerKey;

        private Tuple2<Long, Long> localCount;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //countPerPartition.clear();
        }


        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            // get the state data structure for the per-key state
            countPerKey = context.getKeyedStateStore().getReducingState(
                    new ReducingStateDescriptor<Tuple2<Long, Long>>("perKeyCount", new ReduceFunction<Tuple2<Long, Long>>() {
                        @Override
                        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
                            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                        }
                    }, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    })));

            // get the state data structure for the per-partition state

        }


        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<Long, Long>> input, Collector<Tuple2<Long, Long>> out) throws Exception {
            for (Tuple2<Long, Long> x : input) {
                countPerKey.add(x);
                out.collect(x);
            }
             System.err.println("now "+new Date() +", TimeWindow ="+window+",key = " + tuple + "value = " + countPerKey.get());

        }
    }

    private static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }

    }

    private static class DataSource extends RichParallelSourceFunction<Tuple3<Long, Long,Long>>  {

        private int taskid = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            taskid = getRuntimeContext().getIndexOfThisSubtask();

        }

        @Override
        public void run(SourceContext<Tuple3<Long, Long,Long>> ctx) throws Exception {


            final long startTime = System.currentTimeMillis();

            final long numElements = 10000;
            final long numKeys = 1000;

            for (long i = 0; i < numElements; i++) {
                for (long j = 0; j < numKeys; j++) {
                    ctx.collect(new Tuple3<Long, Long,Long>(numKeys * taskid + j, 1L,System.currentTimeMillis()-new Random().nextInt(10000)));
                }
            }
			final long endTime = System.currentTimeMillis();
			System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");

			SECONDS.sleep(10); // sleep for not close job
			ctx.collectWithTimestamp(new Tuple3<Long, Long,Long>(numKeys * taskid + 1000, 1L,System.currentTimeMillis()),System.currentTimeMillis());
			SECONDS.sleep(10); // sleep for not close job
        }

		@Override
		public void cancel() {
		}

	}

}

package io.github.spafka;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;

public class Utils {

	public static StreamExecutionEnvironment getStreamEnv(){

		String CHECKPOINTS_DIRECTORY = "file:///tmp/state/checkpoint";
		String SAVEPOINT_DIRECTORY = "file:///tmp/state/savepoint";


		try {
			FileUtils.deleteFileOrDirectory(new File("/tmp/state"));
		} catch (IOException e) {

		}


		Configuration configuration = new Configuration();

		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, CHECKPOINTS_DIRECTORY);

		configuration.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, SAVEPOINT_DIRECTORY);
		configuration.setBoolean(CheckpointingOptions.ASYNC_SNAPSHOTS, true);
		configuration.setInteger(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 10);
		configuration.setString(CheckpointingOptions.STATE_BACKEND, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME);
		configuration.setInteger(RestOptions.PORT, 8081);
		configuration.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS,true);
		configuration.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,4); // 4 tm


		StreamExecutionEnvironment localEnvironmentWithWebUI = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

		localEnvironmentWithWebUI.getCheckpointConfig().setCheckpointInterval(10000L);
		localEnvironmentWithWebUI.setBufferTimeout(0L);
		try {
			localEnvironmentWithWebUI.setStateBackend(new RocksDBStateBackend(CHECKPOINTS_DIRECTORY));
		} catch (IOException e) {

		}

		return localEnvironmentWithWebUI;
	}
}

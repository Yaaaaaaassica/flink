package io.github.spafka;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

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






	/**
	 * 统计的文件数量
	 */
	private static long files = 0;
	/**
	 * 代码行数
	 */
	private static int codeLines = 0;
	/**
	 * 注释行数
	 */
	private static int commentLines = 0;
	/**
	 * 空行数量
	 */
	private static int blankLines = 0;
	/**
	 * 文件数组
	 */
	private static ArrayList<File> fileArray = new ArrayList<File>();

	public static void main(String[] args) {

	getCodeNumFromFolder(new File(new File(".").getAbsolutePath().replaceAll("\\.","")).getPath());

	}

	/**
	 * 函数功能：统计指定目录下(文件夹中)java文件中的代码行数
	 *
	 * @param filePath 文件夹路径
	 * @return 代码总行数
	 */
	public static int getCodeNumFromFolder(String filePath) {
		String path = filePath.replace("target/test-classes", "src");

		ArrayList<File> al = getFile(new File(path));
		for (File f : al) {
			// 匹配java格式的文件
			if (f.getName().matches(".*\\.java$")) {
				count(f);
			}
		}
		System.out.println("代码行数：" + codeLines);
		System.out.println("注释行数：" + commentLines);
		System.out.println("空白行数：" + blankLines);
		return codeLines + commentLines + blankLines;
	}

	/**
	 * 函数功能：获得目录下的文件和子目录下的文件
	 *
	 * @param f 目录
	 * @return ArrayList<File>
	 */
	private static ArrayList<File> getFile(File f) {
		File[] ff = f.listFiles();
		if (ff != null) {
			for (File child : ff) {
				if (child.isDirectory()) {
					getFile(child);
				} else {
					fileArray.add(child);
				}
			}
		}
		return fileArray;
	}

	/**
	 * 函数功能：统计具体java文件中的代码行数
	 *
	 * @param f 具体的java文件
	 */
	private static void count(File f) {
		BufferedReader br = null;
		boolean flag = false;
		try {
			br = new BufferedReader(new FileReader(f));
			String line = "";
			while ((line = br.readLine()) != null) {
				// 除去注释前的空格
				line = line.trim();
				// 匹配空行
				if (line.matches("^[ ]*$")) {
					blankLines++;
				} else if (line.startsWith("//")) {
					commentLines++;
				} else if (line.startsWith("/*") && !line.endsWith("*/")) {
					commentLines++;
					flag = true;
				} else if (line.startsWith("/*") && line.endsWith("*/")) {
					commentLines++;
				} else if (flag) {
					commentLines++;
					if (line.endsWith("*/")) {
						flag = false;
					}
				} else {
					codeLines++;
				}
			}
			files++;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 函数功能：获取具体的Java文件中的代码行数
	 *
	 * @param filePath 文件路径
	 * @return 具体文件中的代码行数
	 */
	public static int getCodeNumFromFile(String filePath) {
		File fileName = new File(filePath);
		if (fileName.getName().matches(".*\\.java$")) {
			count(fileName);
		}
		int codeNum = codeLines + blankLines + commentLines;
		System.out.println("代码总行数:" + codeNum);
		return codeNum;
	}

}

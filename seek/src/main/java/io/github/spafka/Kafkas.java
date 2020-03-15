package io.github.spafka;


import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class Kafkas {

	static final String TOPIC = "test";
	static final String CG = "spafka";

	public static void main(String[] args) throws InterruptedException {


		new Thread(() -> {
			try {
				FileUtils.deleteDirectory(new File("/tmp/zookeeper"));

				QuorumPeerConfig config = new QuorumPeerConfig();
				InputStream is = Kafkas.class.getResourceAsStream("/zookeeper.properties");
				Properties p = new Properties();
				p.load(is);
				config.parseProperties(p);
				ServerConfig serverconfig = new ServerConfig();
				serverconfig.readFrom(config);
				new ZooKeeperServerMain().runFromConfig(serverconfig);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (QuorumPeerConfig.ConfigException e) {
				e.printStackTrace();
			}
		}).start();

		TimeUnit.SECONDS.sleep(1);

		IntStream.rangeClosed(1, 3).forEach(x -> {
			new Thread(() -> {
				try {
					FileUtils.deleteDirectory(new File("/var/kafka" + x));


					InputStream is = Kafkas.class.getResourceAsStream("/server" + x + ".properties");
					Properties p = new Properties();
					p.load(is);
					is.close();
					KafkaServerStartable kafkaServerStartable = KafkaServerStartable.fromProps(p);
					kafkaServerStartable.startup();
					kafkaServerStartable.awaitShutdown();
				} catch (IOException e) {

				}


			}).start();
		});


		TimeUnit.SECONDS.sleep(100000);

	}

}

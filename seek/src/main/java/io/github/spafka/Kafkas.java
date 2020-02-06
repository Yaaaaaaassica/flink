package io.github.spafka;


import kafka.server.KafkaServerStartable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Slf4j
public class Kafkas {

	static final String TOPIC = "test";
	static final String CG = "spafka";

	public static void main(String[] args) throws InterruptedException {


		log.info("van ");

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

		TimeUnit.SECONDS.sleep(5);

		CountDownLatch latch = new CountDownLatch(3);

		IntStream.rangeClosed(1, 3).forEach(x -> {
			new Thread(() -> {
				try {


					FileUtils.deleteQuietly(new File("/tmp/kafka" + x));


					InputStream is = Kafkas.class.getResourceAsStream("/server" + x + ".properties");
					Properties p = new Properties();
					p.load(is);
					is.close();
					KafkaServerStartable kafkaServerStartable = KafkaServerStartable.fromProps(p);
					kafkaServerStartable.startup();

					latch.countDown();
					kafkaServerStartable.awaitShutdown();
				} catch (IOException e) {

					log.error("{}", ExceptionUtils.getStackTrace(e));
				}


			}).start();
		});

		latch.await();



		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + 9092);

		AdminClient adminClient = AdminClient.create(props);


		NewTopic test = new NewTopic(TOPIC, 3, (short) 2);
		adminClient.createTopics(Arrays.asList(test)).values().forEach((k,v)->{
			v.whenComplete((a,b)->{
				if (b!=null){
					log.error("{}",b.getMessage());
				}
			});
		});

		TimeUnit.SECONDS.sleep(Long.MAX_VALUE);




	}

}

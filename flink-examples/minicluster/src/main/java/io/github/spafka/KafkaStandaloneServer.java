package io.github.spafka;

import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class KafkaStandaloneServer {

	public static void main(String[] args) throws IOException, QuorumPeerConfig.ConfigException, InterruptedException {


		new Thread(() -> {
			try {
				FileUtils.deleteDirectory(new File("/tmp/zookeeper"));

				QuorumPeerConfig config = new QuorumPeerConfig();
				InputStream is = ZkServer.class.getResourceAsStream("/zookeeper.properties");
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

		new Thread(() -> {

			try {
				FileUtils.deleteDirectory(new File("/tmp/kafka/data"));


				InputStream is = KafkaStandaloneServer.class.getResourceAsStream("/server.properties");
				Properties p = new Properties();
				p.load(is);
				is.close();
				KafkaServerStartable kafkaServerStartable = KafkaServerStartable.fromProps(p);
				kafkaServerStartable.startup();
				kafkaServerStartable.awaitShutdown();
			} catch (IOException e) {

			}


		}).start();

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + 9092);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "test");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer kafkaProducer = new KafkaProducer(props);

		TimeUnit.SECONDS.sleep(10);
		while (true) {

			Scanner scanner = new Scanner(System.in);
			String line = scanner.nextLine();

			kafkaProducer.send(new ProducerRecord("flink", null, line));

		}

	}
}

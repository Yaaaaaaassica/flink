package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;

import java.net.InetSocketAddress;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.*;

public class FlinkNetty {

	public static void main(String[] args) throws Exception {

		ResultPartitionManager partitionProvider = new ResultPartitionManager();
		TaskEventDispatcher taskEventPublisher = new TaskEventDispatcher();
		NettyProtocol protocol = new NettyProtocol(
			partitionProvider,
			taskEventPublisher);



		// We need a real server and client in this test, because Netty's EmbeddedChannel is
		// not failing the ChannelPromise of failed writes.
		NettyConfig config = createConfig();
		NettyTestUtil.NettyServerAndClient serverAndClient = initServerAndClient(protocol, config);

		NettyClient client = serverAndClient.client();
		NettyServer server = serverAndClient.server();

		Channel ch = connect(serverAndClient);

		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		PartitionRequestClientFactory partitionRequestClientFactory = new PartitionRequestClientFactory(client);
		InetSocketAddress address = new InetSocketAddress(config.getServerPort());
		ConnectionID connectionId = new ConnectionID(address, 1);


		NettyPartitionRequestClient nettyPartitionRequestClient = new NettyPartitionRequestClient(
			ch,
			handler,
			connectionId,partitionRequestClientFactory);

		PartitionRequestClient requestClient = new NettyPartitionRequestClient(
			ch, handler, connectionId,partitionRequestClientFactory);

		SingleInputGate inputGate = new SingleInputGate(
			"van",
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			0,
			1,
			null,
			null,
			null);
		//RemoteInputChannel remoteInputChannel = new RemoteInputChannel(inputGate);

		//requestClient.requestSubpartition(new ResultPartitionID(),1,);

	}


	// ---------------------------------------------------------------------------------------------
	// Helpers
	// ---------------------------------------------------------------------------------------------


	private NetworkClientHandler getClientHandler(Channel ch) {
		return ch.pipeline().get(NetworkClientHandler.class);
	}


}

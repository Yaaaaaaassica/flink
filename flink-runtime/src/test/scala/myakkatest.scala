import java.util.concurrent.{CompletableFuture, TimeUnit}

import akka.actor.ActorSystem
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.rpc.akka.{AkkaRpcService, AkkaRpcServiceConfiguration}
import org.apache.flink.runtime.rpc.{RpcEndpoint, RpcGateway, RpcService}
import org.junit.Test

trait FooGateWay extends RpcGateway {

  def foo: String
}

trait BarGateWay extends RpcGateway {

  def bar: String
}

class FooEndPoint(rpcService: RpcService, endpointId: String)
  extends RpcEndpoint(rpcService: RpcService, endpointId: String)
    with FooGateWay {


  override def getAddress: String = rpcService.getAddress

  override def getHostname: String = rpcService.getAddress

  override def foo = {

    println(s"in fooendpoint ")
    "foo"
  }
}


class FooEndPointTest {


  @Test
  def testconnect = {

    val actorSystem = AkkaUtils.createDefaultActorSystem

    val timeout = Time.seconds(10L)

    val akkaRpcService = new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.defaultConfiguration)
    val remote = new FooEndPoint(new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.defaultConfiguration), "fooEndPoint")

    remote.start()


    val actorSystem2: ActorSystem =
      AkkaUtils.createDefaultActorSystem()

    val address = s"akka.tcp://flink@${remote.getRpcService.getAddress}:${remote.getRpcService.getPort}/user/fooEndPoint";

    val fooGateWay: CompletableFuture[FooGateWay] =
      new AkkaRpcService(actorSystem2, AkkaRpcServiceConfiguration.defaultConfiguration)
        .connect(address, classOf[FooGateWay])
    val gateWay = fooGateWay.get()

    println(gateWay.foo)


  }


}


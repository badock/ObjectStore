package org.discovery.dht



import org.discovery.AkkaArc.util.{FakeNetworkLocation, NodeRef}
import akka.actor.ActorSystem
import org.discovery.dht.system.{Version, DistributedHashTableService, DistributedHashTableFactory}
import scala.concurrent.Await
import scala.concurrent.duration._

object Test extends App {

  val system = ActorSystem(s"LocalTestSystem")



  val mainDhtService = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation( Util.md5("0")), system.deadLetters))
  var services: List[DistributedHashTableService] = List(mainDhtService)

  for (i <- 1 to 20) {
    val dhtService = DistributedHashTableFactory.createLocalService(system, NodeRef(FakeNetworkLocation(Util.md5(s"$i")), system.deadLetters))
    dhtService.connect(mainDhtService)
    services = services ::: List(dhtService)
  }


  Thread.sleep(6000)

  val MAX_LOOP: Int = 30000
  var count: Int = 0

  for (i <- 0 to MAX_LOOP-1) {
    val s = services(i % services.size)
    s.put(s"$i", i)
  }

  Thread.sleep(3000)

  for (i <- 0 to MAX_LOOP-1) {
    val s = services(i % services.size)
    val version = Await.result(s.get(s"$i"), 2 seconds)
    version match {
      case Some(Version(_, j: Int, _)) if(i == j) =>
        count += 1
        true
      case _ =>
        false
    }
  }

  println(s"$count / $MAX_LOOP")

  system.awaitTermination()

}

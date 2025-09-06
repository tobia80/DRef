package io.github.tobia80.cref.raft

import zio.{Task, ZIO, ZLayer}

trait IpProvider {
  def findNodeAddresses(): Task[List[String]]

  def findMyAddress(): Task[String]
}

object IpProvider { // TODO use k8s to retrieve ips for one service

  def static(myIp: String, allIps: List[String]): ZLayer[Any, Nothing, IpProvider] = ZLayer.succeed {
    new IpProvider {
      override def findNodeAddresses(): Task[List[String]] = ZIO.succeed(allIps)

      override def findMyAddress(): Task[String] = ZIO.succeed(myIp)
    }
  }

  def dnsBased(service: String): ZLayer[Any, Nothing, IpProvider] = ZLayer.succeed {
    new IpProvider {
      override def findNodeAddresses(): Task[List[String]] = ZIO.attempt {
        import java.net.InetAddress
        InetAddress.getAllByName(service).map(_.getHostAddress).toList
      }

      override def findMyAddress(): Task[String] = ZIO.attempt {
        import java.net.InetAddress
        InetAddress.getLocalHost.getHostAddress
      }
    }
  }

}

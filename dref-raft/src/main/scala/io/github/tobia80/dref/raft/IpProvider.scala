package io.github.tobia80.dref.raft

import zio.{Task, ZEnvironment, ZIO, ZLayer}

trait IpProvider {
  def findNodeAddresses(): Task[List[String]]

  def findMyAddress(): Task[String]

  def expectedEndpoints: Task[Int]
}

object IpProvider {

  def k8s(
      serviceName: String,
      myAddress: String,
      namespace: String
  ): ZLayer[Any, Throwable, IpProvider] =
    KubernetesIpProvider.live(serviceName, namespace).map { env =>
      val delegate = env.get[IpProvider]
      ZEnvironment(new IpProvider {
        override def findNodeAddresses(): Task[List[String]] = delegate.findNodeAddresses()
        override def findMyAddress(): Task[String]           = ZIO.succeed(myAddress)
        override def expectedEndpoints: Task[Int]            = delegate.expectedEndpoints
      })
    }

  def local: ZLayer[Any, Nothing, IpProvider] = ZLayer.succeed {
    new IpProvider {
      override def findNodeAddresses(): Task[List[String]] = ZIO.succeed(List("127.0.0.1"))

      override def findMyAddress(): Task[String] = ZIO.succeed("127.0.0.1")

      override def expectedEndpoints: Task[Int] = ZIO.succeed(1)
    }
  }

  def static(ips: String*): ZLayer[Any, Nothing, IpProvider] = ZLayer.succeed {
    new IpProvider {
      override def findNodeAddresses(): Task[List[String]] = ZIO.succeed(ips.toList)

      override def findMyAddress(): Task[String] = ZIO.attempt {
        import java.net.InetAddress
        InetAddress.getLocalHost.getHostAddress
      }

      override def expectedEndpoints: Task[Int] = ZIO.succeed(ips.size)
    }
  }

  def dnsBased(services: String*): ZLayer[Any, Nothing, IpProvider] = ZLayer.succeed {
    new IpProvider {
      override def findNodeAddresses(): Task[List[String]] = ZIO.attempt {
        import java.net.InetAddress
        services.flatMap { service =>
          InetAddress.getAllByName(service).map(_.getHostAddress)
        }.toList
      }

      override def findMyAddress(): Task[String] = ZIO.attempt {
        import java.net.InetAddress
        InetAddress.getLocalHost.getHostAddress
      }

      override def expectedEndpoints: Task[Int] = findNodeAddresses().map(_.size)
    }
  }

}

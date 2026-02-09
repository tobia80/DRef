package io.github.tobia80.dref.raft

import com.coralogix.zio.k8s.client.config.*
import com.coralogix.zio.k8s.client.config.httpclient.*
import com.coralogix.zio.k8s.client.v1.endpoints.Endpointses
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.client.K8sFailure
import zio.*

import java.net.{InetAddress, NetworkInterface}
import scala.jdk.CollectionConverters.*

object KubernetesIpProvider {

  private def k8sFailureToThrowable(failure: K8sFailure): Throwable =
    RuntimeException(s"Kubernetes API error: $failure")

  private def localIps(): Set[String] =
    NetworkInterface.getNetworkInterfaces.asScala
      .flatMap(_.getInetAddresses.asScala)
      .filterNot(_.isLoopbackAddress)
      .map(_.getHostAddress)
      .toSet

  def live(
      serviceName: String,
      namespace: String
  ): ZLayer[Any, Throwable, IpProvider] =
    ZLayer.scoped {
      for {
        endpointsSvc <- ZIO
                          .service[Endpointses]
                          .provideLayer(k8sDefault >>> Endpointses.live)
      } yield create(serviceName, namespace, endpointsSvc)
    }

  private[raft] def create(
      serviceName: String,
      namespace: String,
      endpointsSvc: Endpointses
  ): IpProvider = {
    val k8sNamespace = K8sNamespace(namespace)
    new IpProvider {

      override def findNodeAddresses(): Task[List[String]] =
        endpointsSvc
          .get(serviceName, k8sNamespace)
          .mapError(k8sFailureToThrowable)
          .map { ep =>
            val subsets = ep.subsets.toOption.getOrElse(Vector.empty)
            subsets.flatMap { subset =>
              subset.addresses.toOption.getOrElse(Vector.empty).map(_.ip)
            }.toList
          }

      override def findMyAddress(): Task[String] =
        findNodeAddresses().flatMap { endpointIps =>
          ZIO.attempt {
            val myIps = localIps()
            endpointIps.find(myIps.contains)
          }.flatMap {
            case Some(ip) => ZIO.succeed(ip)
            case None     => ZIO.attempt(InetAddress.getLocalHost.getHostAddress)
          }
        }

      override def expectedEndpoints: Task[Int] =
        findNodeAddresses().map(_.size)
    }
  }
}

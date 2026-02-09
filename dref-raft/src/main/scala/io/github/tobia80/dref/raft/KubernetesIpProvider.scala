package io.github.tobia80.dref.raft

import com.coralogix.zio.k8s.client.config.*
import com.coralogix.zio.k8s.client.config.httpclient.*
import com.coralogix.zio.k8s.client.v1.endpoints.Endpointses
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.client.K8sFailure
import zio.*

object KubernetesIpProvider {

  private def k8sFailureToThrowable(failure: K8sFailure): Throwable =
    RuntimeException(s"Kubernetes API error: $failure")

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

      override def findMyAddress(): Task[String] = ZIO.attempt {
        java.net.InetAddress.getLocalHost.getHostAddress
      }

      override def expectedEndpoints: Task[Int] =
        findNodeAddresses().map(_.size)
    }
  }
}

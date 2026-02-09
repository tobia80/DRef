package io.github.tobia80.dref.raft

import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.client.v1.endpoints.Endpointses
import com.coralogix.zio.k8s.model.core.v1.{EndpointAddress, EndpointSubset, Endpoints as K8sEndpoints}
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import zio.*
import zio.prelude.data.Optional
import zio.test.{assertTrue, Spec, TestEnvironment, ZIOSpecDefault}

object KubernetesIpProviderSpec extends ZIOSpecDefault {

  private val serviceName = "my-service"
  private val namespace   = "default"

  private def makeEndpoints(name: String, ips: List[String]): K8sEndpoints =
    K8sEndpoints(
      metadata = Optional.Present(ObjectMeta(name = Optional.Present(name))),
      subsets = Optional.Present(
        Vector(
          EndpointSubset(
            addresses = Optional.Present(ips.map(ip => EndpointAddress(ip = ip)).toVector)
          )
        )
      )
    )

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("KubernetesIpProvider")(
    test("findNodeAddresses should return IPs from Endpoints resource") {
      for {
        svc      <- ZIO.service[Endpointses]
        _        <- svc.create(makeEndpoints(serviceName, List("10.0.0.1", "10.0.0.2", "10.0.0.3")), K8sNamespace(namespace))
                       .mapError(e => RuntimeException(s"Failed to create test endpoints: $e"))
        provider  = KubernetesIpProvider.create(serviceName, namespace, svc)
        ips      <- provider.findNodeAddresses()
      } yield assertTrue(ips == List("10.0.0.1", "10.0.0.2", "10.0.0.3"))
    },
    test("findMyAddress should return local host address") {
      for {
        svc      <- ZIO.service[Endpointses]
        _        <- svc.create(makeEndpoints(serviceName, List("10.0.0.1", "10.0.0.2")), K8sNamespace(namespace))
                       .mapError(e => RuntimeException(s"Failed to create test endpoints: $e"))
        provider  = KubernetesIpProvider.create(serviceName, namespace, svc)
        myAddr   <- provider.findMyAddress()
      } yield assertTrue(myAddr.nonEmpty)
    },
    test("expectedEndpoints should return the count of IPs") {
      for {
        svc      <- ZIO.service[Endpointses]
        _        <- svc.create(makeEndpoints(serviceName, List("10.0.0.1", "10.0.0.2")), K8sNamespace(namespace))
                       .mapError(e => RuntimeException(s"Failed to create test endpoints: $e"))
        provider  = KubernetesIpProvider.create(serviceName, namespace, svc)
        count    <- provider.expectedEndpoints
      } yield assertTrue(count == 2)
    },
    test("findNodeAddresses should handle multiple subsets") {
      val endpoints = K8sEndpoints(
        metadata = Optional.Present(ObjectMeta(name = Optional.Present("multi-subset-svc"))),
        subsets = Optional.Present(
          Vector(
            EndpointSubset(
              addresses = Optional.Present(Vector(EndpointAddress(ip = "10.0.1.1"), EndpointAddress(ip = "10.0.1.2")))
            ),
            EndpointSubset(
              addresses = Optional.Present(Vector(EndpointAddress(ip = "10.0.2.1")))
            )
          )
        )
      )
      for {
        svc      <- ZIO.service[Endpointses]
        _        <- svc.create(endpoints, K8sNamespace(namespace))
                       .mapError(e => RuntimeException(s"Failed to create test endpoints: $e"))
        provider  = KubernetesIpProvider.create("multi-subset-svc", namespace, svc)
        ips      <- provider.findNodeAddresses()
      } yield assertTrue(ips == List("10.0.1.1", "10.0.1.2", "10.0.2.1"))
    },
    test("findNodeAddresses should return empty list when no subsets") {
      val endpoints = K8sEndpoints(
        metadata = Optional.Present(ObjectMeta(name = Optional.Present("empty-svc")))
      )
      for {
        svc      <- ZIO.service[Endpointses]
        _        <- svc.create(endpoints, K8sNamespace(namespace))
                       .mapError(e => RuntimeException(s"Failed to create test endpoints: $e"))
        provider  = KubernetesIpProvider.create("empty-svc", namespace, svc)
        ips      <- provider.findNodeAddresses()
      } yield assertTrue(ips.isEmpty)
    }
  ).provide(Endpointses.test)
}

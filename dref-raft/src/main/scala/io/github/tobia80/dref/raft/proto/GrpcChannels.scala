package io.github.tobia80.dref.raft.proto

import io.grpc.netty.NettyChannelBuilder
import scalapb.zio_grpc.ZManagedChannel

object GrpcChannels {
  def managedChannel(address: String): ZManagedChannel = {
    val Array(host, port) = address.split(":", 2)
    ZManagedChannel(NettyChannelBuilder.forAddress(host, port.toInt).usePlaintext())
  }
}

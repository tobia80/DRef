package io.github.tobia80.dref.raft.proto

import zio.{Ref, Scope, ZIO}

/** Keep a `Ref[Map[id, client]]` aligned with a discovery snapshot. */
private[proto] object PeerMapSync {

  def sync[C](
    ref: Ref[Map[String, C]],
    endpoints: List[NodeEndpoint],
    mkClient: NodeEndpoint => ZIO[Scope, Throwable, C]
  ): ZIO[Scope, Throwable, Unit] =
    for {
      current <- ref.get
      desired  = endpoints.map(ep => ep.id).toSet
      toRemove = current.keySet -- desired
      toAdd    = endpoints.filter(ep => !current.contains(ep.id))
      _       <- ZIO.foreachDiscard(toRemove)(id => ref.update(_ - id))
      added   <- ZIO.foreach(toAdd)(ep => mkClient(ep).map(ep.id -> _))
      _       <- ref.update(_ ++ added.toMap).when(added.nonEmpty)
    } yield ()
}

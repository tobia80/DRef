package io.github.tobia80.dref.redis

import io.github.tobia80.dref.*
import io.lettuce.core.{ClientOptions, RedisURI, SslOptions}
import zio.*
import zio.schema.{DeriveSchema, Schema}
import zio.stream.{Take, ZStream}

case class RedisConfig(
  host: String,
  port: Int,
  database: Int,
  username: Option[String],
  password: Option[String],
  caCert: Option[String],
  ttl: Option[Duration]
) {

  def toRedisURI: String = {
    val builder = RedisURI.Builder
      .redis(host, port)
      .withDatabase(database)
      .withSsl(caCert.isDefined)

    (username, password) match {
      case (Some(u), Some(p)) => builder.withAuthentication(u, p.toCharArray)
      case (None, Some(p))    => builder.withPassword(p.toCharArray)
      case _                  => ()
    }

    builder
      .build()
      .toURI
      .toString
  }

  def toOptions: ClientOptions = {
    val clientBuilder = ClientOptions.builder()
    caCert.foreach { cert =>
      clientBuilder.sslOptions(
        SslOptions.builder().trustManager(SslOptions.Resource.from(java.io.File(cert))).build()
      )
    }
    clientBuilder.build()
  }
}

object RedisConfig {

  def get: URIO[RedisConfig, RedisConfig] = ZIO.service[RedisConfig]
}

object RedisDRefContext {

  private case class ChangePayload(name: Chunk[Byte], value: Chunk[Byte], delete: Boolean = false) {

    def nameString: String =
      new String(name.toArray)
  }

  private given Schema[ChangePayload] = DeriveSchema.gen[ChangePayload]

  private given DRefCodec[ChangePayload] = DRef.msgpack.derived[ChangePayload] // TODO make it configurable?

  private val channel = Chunk.fromArray("dref-change".getBytes)

  val live: ZLayer[zio.Scope & RedisConfig, Throwable, DRefContext] = ZLayer(
    for {
      config           <- ZIO.service[RedisConfig]
      redisClient      <- RedisClient.make(config)
      redisSubscription = redisClient.subscribe(channel)
      hub              <- Hub.bounded[Take[Throwable, ChangePayload]](64)
      _                <- redisSubscription
                            .mapZIO { value =>
                              DRefCodec
                                .deserializeFromArray[ChangePayload](value.toArray)
                                .tapError { error =>
                                  ZIO.logError(s"Cannot decode notification ($value) (Error: $error)")
                                }
                                .either
                            }
                            .collect { case Right(notification) => notification }
                            .runForeach { event =>
                              ZIO.logDebug(s"Publishing event $event to hub") *> hub
                                .publish(Take.single(event))
                            }
                            .forkDaemon
    } yield new DRefContext {
      private def publish(message: Chunk[Byte]) = redisClient.publish(channel, message)

      override def defaultTtl: Duration = config.ttl.getOrElse(20.seconds)

      override def setElement(name: String, a: Array[Byte], ttl: Option[Duration]): Task[Unit] = {
        val change = ChangePayload(name = Chunk.fromArray(name.getBytes), value = Chunk.fromArray(a))
        for {
          value <-
            DRefCodec
              .serializeToArray[ChangePayload](change)
              .mapError(err => new Exception(s"Cannot serialize notification ($change) (Error: ${err.getMessage})"))
          _     <- redisClient.set(Chunk.fromArray(name.getBytes), Chunk.fromArray(a), ttl)
          _     <- publish(Chunk.fromArray(value))
        } yield ()
      }

      override def deleteElement(name: String): Task[Unit] = {
        val delete = ChangePayload(name = Chunk.fromArray(name.getBytes), value = Chunk.empty, delete = true)
        for {
          value <-
            DRefCodec
              .serializeToArray[ChangePayload](delete)
              .mapError(err => new Exception(s"Cannot serialize notification ($delete) (Error: ${err.getMessage})"))
          _     <- redisClient.del(Chunk.fromArray(name.getBytes))
          _     <- publish(Chunk.fromArray(value))
        } yield ()
      }

      import zio.Duration
      override def keepAliveStream(name: String, ttl: Duration): ZStream[Any, Throwable, Unit] = {
        val ttlZio = Duration.fromScala(ttl.asFiniteDuration / 1.25)
        ZStream.repeatZIOWithSchedule(keepAlive(name, ttl), Schedule.fixed(ttlZio))
      }

      private def keepAlive(name: String, ttl: Duration): Task[Unit] =
        redisClient.expire(Chunk.fromArray(name.getBytes), ttl).unit

      override def setElementIfNotExist(name: String, value: Array[Byte], ttl: Option[Duration]): Task[Boolean] = {
        val change = ChangePayload(name = Chunk.fromArray(name.getBytes), value = Chunk.fromArray(value))
        for {
          value  <-
            DRefCodec
              .serializeToArray[ChangePayload](change)
              .mapError(err => new Exception(s"Cannot serialize notification ($change) (Error: ${err.getMessage})"))
          result <- redisClient.setNx(Chunk.fromArray(name.getBytes), Chunk.fromArray(value))
          _      <- ttl.fold(ZIO.unit)(keepAlive(name, _)).when(result)
          _      <- publish(Chunk.fromArray(value)).when(result)
        } yield result
      }

      override def getElement(name: String): Task[Option[Array[Byte]]] =
        redisClient.get(Chunk.fromArray(name.getBytes)).map(_.map(_.toArray))

      override def onChangeStream(name: String): ZStream[Any, Throwable, ChangeEvent] =
        ZStream.logDebug(s"Subscribing to $name") *> ZStream.fromHub(hub).flattenTake.collect {
          case c @ ChangePayload(_, value, false) if c.nameString == name => SetElement(name, value.toArray)
          case c @ ChangePayload(_, value, true) if c.nameString == name  => DeleteElement(name)
        }

      override def detectDeletionFromUnderlyingStream(
        name: String
      ): ZStream[Any, Throwable, DeleteElement] = {
        val notExist = for {
          result <- redisClient.get(Chunk.fromArray(name.getBytes))
        } yield result.isEmpty
        ZStream.repeatZIO(notExist.delay(1.second)).filter(identity).as(DeleteElement(name))
      }
    }
  )
}

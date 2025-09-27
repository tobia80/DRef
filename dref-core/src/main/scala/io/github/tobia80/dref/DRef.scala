package io.github.tobia80.dref

import io.github.vigoo.desert.*
import io.github.vigoo.desert.zioschema.DerivedBinaryCodec
import zio.ZIO.fromEither
import zio.internal.stacktracer.Tracer.*
import zio.schema.{DeriveSchema, Schema}
import zio.stream.ZStream
import zio.{
  duration2DurationOps,
  durationInt,
  Chunk,
  Clock,
  Duration,
  Promise,
  Queue,
  RIO,
  Random,
  Ref,
  Schedule,
  Scope,
  Task,
  Trace,
  ULayer,
  ZIO,
  ZLayer
}

import java.util.concurrent.TimeUnit
import scala.deriving.Mirror

trait DRef[T] {

  def get: Task[T]

  def set(a: T): Task[Unit]

  def setIfNotExist(a: T): Task[Boolean]

  def onChange[R](a: T => Task[R]): Task[Unit]

  def changeStream: ZStream[Any, Throwable, T]

  def modify[B](f: T => (B, T)): Task[B]

  def modifyZIO[B, C](f: T => RIO[C, (B, T)]): RIO[C, B]

  def getAndUpdate(f: T => T): Task[T] =
    modify(v => (v, f(v)))

  def update(f: T => T): Task[Unit] =
    modify(v => ((), f(v)))

  def updateAndGet(f: T => T): Task[T] =
    modify { v =>
      val result = f(v)
      (result, result)
    }

  def getAndUpdateZIO[C](f: T => RIO[C, T]): RIO[C, T] =
    modifyZIO { v =>
      for {
        result <- f(v)
      } yield (v, result)
    }

  def updateZIO[C](f: T => RIO[C, T]): RIO[C, Unit] =
    modifyZIO { v =>
      for {
        result <- f(v)
      } yield ((), result)
    }

  def updateAndGetZIO[C](f: T => RIO[C, T]): RIO[C, T] =
    modifyZIO { v =>
      for {
        result <- f(v)
      } yield (result, result)
    }

}

trait DRefContext {

  def defaultTtl: Duration

  def setElement(name: String, value: Array[Byte], ttl: Option[Duration]): Task[Unit]

  def setElementIfNotExist(name: String, value: Array[Byte], ttl: Option[Duration]): Task[Boolean]

  def keepAliveStream(name: String, ttl: Duration): ZStream[Any, Throwable, Unit]

  def getElement(name: String): Task[Option[Array[Byte]]]

  def deleteElement(name: String): Task[Unit]

  def onChangeStream(name: String): ZStream[Any, Throwable, ChangeEvent]

  def detectDeletionFromUnderlyingStream(
    name: String
  ): ZStream[Any, Throwable, DeleteElement]

}

sealed trait ChangeEvent {
  def name: String
}

case class SetElement(name: String, value: Array[Byte]) extends ChangeEvent
case class DeleteElement(name: String) extends ChangeEvent

private case class ExpiringValue(value: Array[Byte], expireAt: Option[Long])

object DRefContext {

  private def removeExpiredElements(ref: Ref[Map[String, ExpiringValue]]): ZIO[Any, Nothing, Unit] =
    for {
      now <- Clock.currentTime(TimeUnit.MILLISECONDS)
      _   <- ref.update { old =>
               old.filter { case (_, v) =>
                 v.expireAt.forall(_ > now)
               }
             }
    } yield ()

  val local: ZLayer[Scope, Nothing, DRefContext] = ZLayer(for {
    ref     <- Ref.make[Map[String, ExpiringValue]](Map.empty)
    changes <- Queue.sliding[ChangeEvent](2)
    _       <- removeExpiredElements(ref)
                 .repeat(Schedule.fixed(200.millis))
                 .forkScoped
  } yield new DRefContext {

    override def setElement(name: String, value: Array[Byte], ttl: Option[Duration]): Task[Unit] =
      Clock.currentTime(TimeUnit.MILLISECONDS).flatMap { now =>
        val expireAt = ttl.map(t => now + t.toMillis)
        ref.update { el =>
          el.updated(name, ExpiringValue(value, expireAt))
        } *> changes.offer(SetElement(name, value)).unit
      }

    override def getElement(name: String): Task[Option[Array[Byte]]] =
      ref.get.map(_.get(name).map(_.value))

    override def keepAliveStream(name: String, ttl: Duration): ZStream[Any, Throwable, Unit] = {
      val ttlZio = Duration.fromScala(ttl.asFiniteDuration / 1.25)
      ZStream.repeatZIOWithSchedule(keepAlive(name, ttl), Schedule.fixed(ttlZio))
    }

    private def keepAlive(name: String, ttl: Duration): Task[Unit] =
      Clock.currentTime(TimeUnit.MILLISECONDS).flatMap { now =>
        ref.update { old =>
          old.get(name) match {
            case Some(value) =>
              old.updated(name, value.copy(expireAt = Some(now + ttl.toMillis)))
            case None        => old
          }
        }
      }

    override def onChangeStream(name: String): ZStream[Any, Throwable, ChangeEvent] =
      ZStream.fromQueue(changes).filter(_.name == name)

    override def setElementIfNotExist(name: String, value: Array[Byte], ttl: Option[Duration]): Task[Boolean] =
      Clock
        .currentTime(TimeUnit.MILLISECONDS)
        .flatMap(now =>
          ref
            .modify { el =>
              val contains = el.contains(name)
              if !contains then true -> el.updated(name, ExpiringValue(value, ttl.map(t => now + t.toMillis)))
              else false             -> el
            }
            .tap(result => changes.offer(SetElement(name, value)).when(result))
        )

    override def deleteElement(name: String): Task[Unit] = ref.update { el =>
      el.removed(name)
    } *> changes.offer(DeleteElement(name)).unit

    override def defaultTtl: Duration = 5.seconds

    override def detectDeletionFromUnderlyingStream(
      name: String
    ): ZStream[Any, Throwable, DeleteElement] = ZStream.never
  })
}

object DRef {

  export io.github.vigoo.desert.{bigDecimalCodec, booleanCodec, stringCodec}

  object auto {
    implicit inline def derived[T: Mirror.Of]: BinaryCodec[T] = DerivedBinaryCodec.derive[T]

    implicit inline def derivedSchema[T: Mirror.Of]: Schema[T] = DeriveSchema.gen[T]

  }

  import java.security.MessageDigest

  private def sha(s: String): Task[String] =
    ZIO.attempt {
      val instance = MessageDigest.getInstance("SHA-256")
      val value = instance.digest(s.getBytes)
      val out: java.lang.StringBuilder = new java.lang.StringBuilder
      for b <- value do out.append(String.format("%02X", b))
      out.toString
    }

  def lockWithContext[R, A, T](context: DRefContext, id: IdProvider = AutoId)(
    f: => RIO[R, T]
  )(implicit trace: Trace): RIO[R, T] =
    for {
      stack                <- ZIO.stackTrace
      hash                 <- sha(stack.prettyPrint)
      traceInfo            <-
        ZIO.fromOption(instance.unapply(trace)).orElseFail(new Throwable("No trace available"))
      a                    <- Random.nextLong
      bytes                <- fromEither(serializeToArray(a)).mapError(failure => new Throwable(failure.message))
      location              = traceInfo._1
      file                  = traceInfo._2
      line                  = traceInfo._3
      name                  = id match {
                                case AutoId       => s"$location:$file:$line ($hash)"
                                case ManualId(id) => id
                              }
      defaultTtl            = context.defaultTtl
      gainedLock           <- context.setElementIfNotExist(name, bytes, Some(defaultTtl))
      interruptStream      <- Promise.make[Throwable, Unit]
      aliveInterruptStream <- Promise.make[Throwable, Unit]
      _                    <- ZStream
                                .mergeAll(2)(
                                  context // attempt to gain lock when delete element happens, if not keep listening otherwise terminate stream
                                    .onChangeStream(name),
                                  context.detectDeletionFromUnderlyingStream(name)
                                )
                                .interruptWhen(interruptStream)
                                .collectZIO { case DeleteElement(name) => // works when lock is lost?
                                  for {
                                    _          <- ZIO.logInfo(s"Lock $name was deleted, trying to gain it")
                                    lockedGain <- context.setElementIfNotExist(name, bytes, Some(defaultTtl))
                                    _          <- interruptStream.succeed(()).when(lockedGain)
                                  } yield ()
                                }
                                .runDrain
                                .unless(gainedLock)
      _                    <- ZIO.logInfo(s"Lock $name acquired")
      aliveFiber           <- context.keepAliveStream(name, defaultTtl).interruptWhen(aliveInterruptStream).runDrain.fork
      result               <- f.ensuring {
                                aliveInterruptStream.succeed(()) *>
                                  context
                                    .deleteElement(name)
                                    .tapBoth(
                                      err => ZIO.logError(s"Error releasing lock $name: ${err.getMessage}"),
                                      _ => ZIO.logInfo(s"Lock $name released")
                                    )
                                    .ignore
                              }
    } yield result

  def lock[R, A, T](id: IdProvider = AutoId)(f: => RIO[R, T])(implicit trace: Trace): RIO[DRefContext & R, T] =
    ZIO.serviceWithZIO[DRefContext](context => lockWithContext(context, id)(f))

  def make[T: BinaryCodec](a: => T, id: IdProvider = AutoId)(implicit trace: Trace): RIO[DRefContext, DRef[T]] =
    for {
      context   <- ZIO.service[DRefContext]
      stack     <- ZIO.stackTrace
      hash      <- sha(stack.prettyPrint)
      traceInfo <-
        ZIO.fromOption(instance.unapply(trace)).orElseFail(new Throwable("No trace available"))
      bytes     <- fromEither(serializeToArray(a)).mapError(failure => new Throwable(failure.message))
      location   = traceInfo._1
      file       = traceInfo._2
      line       = traceInfo._3
      name       = id match {
                     case AutoId       => s"$location:$file:$line ($hash)"
                     case ManualId(id) => id
                   }
      _         <- context.setElementIfNotExist(name, bytes, None)
    } yield new DRefImpl[T](context)(name)
}

sealed trait IdProvider

case object AutoId extends IdProvider
case class ManualId(value: String) extends IdProvider

class DRefImpl[T: BinaryCodec](context: DRefContext)(name: String) extends DRef[T] {

  override def get: Task[T] = {
    val result: Task[Option[Array[Byte]]] = context.getElement(name)
    result.flatMap {
      case Some(bytes) =>
        fromEither(deserializeFromArray[T](bytes)).mapError(failure => new Throwable(failure.message))
      case None        => ZIO.fail(new Throwable(s"Element $name not found"))
    }
  }

  override def set(a: T): Task[Unit] = fromEither(serializeToArray(a))
    .mapError(failure => new Throwable(failure.message))
    .flatMap(context.setElement(name, _, None))

  override def onChange[R](a: T => Task[R]): Task[Unit] = changeStream.foreach(a).fork.unit

  override def changeStream: ZStream[Any, Throwable, T] =
    context.onChangeStream(name).collectZIO { case SetElement(_, elementValue) =>
      val value = deserializeFromArray[T](elementValue)
      fromEither(value).mapError(failure => new Throwable(failure.message))
    }

  override def setIfNotExist(a: T): Task[Boolean] =
    fromEither(serializeToArray(a))
      .mapError(failure => new Throwable(failure.message))
      .flatMap(context.setElementIfNotExist(name, _, None))

  override def modify[B](f: T => (B, T)): Task[B] =
    DRef.lockWithContext(context, ManualId(s"lock:$name")) {
      for {
        current      <- get
        (b, newValue) = f(current)
        _            <- set(newValue)
      } yield b
    }

  override def modifyZIO[B, C](f: T => RIO[C, (B, T)]): RIO[C, B] =
    DRef.lockWithContext(context, ManualId(s"lock:$name")) {
      for {
        current       <- get
        (b, newValue) <- f(current)
        _             <- set(newValue)
      } yield b
    }

}

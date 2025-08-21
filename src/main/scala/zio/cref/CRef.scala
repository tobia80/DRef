package zio.cref

import io.github.vigoo.desert.*
import io.github.vigoo.desert.zioschema.DerivedBinaryCodec
import zio.ZIO.fromEither
import zio.internal.stacktracer.Tracer.*
import zio.schema.{DeriveSchema, Schema}
import zio.stream.ZStream
import zio.{Promise, Queue, RIO, Random, Ref, Schedule, Task, Trace, ULayer, ZIO, ZLayer}

import scala.deriving.Mirror

trait CRef[T] {

  def get: RIO[CRefContext, T]

  def set(a: T): RIO[CRefContext, Unit]

  def setIfNotExist(a: T): RIO[CRefContext, Boolean]

  def onChange[R](a: T => Task[R]): RIO[CRefContext, Unit]

  def changeStream: ZStream[CRefContext, Throwable, T]

}

trait CRefContext {

  def setElement(name: String, value: Array[Byte]): Task[Unit]

  def setElementIfNotExist(name: String, value: Array[Byte]): Task[Boolean]

  def keepAliveStream(name: String): ZStream[Any, Throwable, Unit]

  def getElement(name: String): Task[Option[Array[Byte]]]

  def deleteElement(name: String): Task[Unit]

  def onChangeStream(name: String): ZStream[Any, Throwable, ChangeEvent]

}

sealed trait ChangeEvent {
  def name: String
}

case class SetElement(name: String, value: Array[Byte]) extends ChangeEvent
case class DeleteElement(name: String) extends ChangeEvent

object CRefContext {

  val local: ULayer[CRefContext] = ZLayer(for {
    ref     <- Ref.make[Map[String, Array[Byte]]](Map.empty)
    changes <- Queue.sliding[ChangeEvent](2)
  } yield new CRefContext {

    override def setElement(name: String, value: Array[Byte]): Task[Unit] = ref.update { el =>
      el.updated(name, value)
    } *> changes.offer(SetElement(name, value)).unit

    override def getElement(name: String): Task[Option[Array[Byte]]] =
      ref.get.map(_.get(name))

    override def keepAliveStream(name: String): ZStream[Any, Throwable, Unit] = ZStream.empty

    override def onChangeStream(name: String): ZStream[Any, Throwable, ChangeEvent] =
      ZStream.fromQueue(changes).filter(_.name == name)

    override def setElementIfNotExist(name: String, value: Array[Byte]): Task[Boolean] = ref
      .modify { el =>
        val contains = el.contains(name)
        if !contains then true -> el.updated(name, value)
        else false             -> el
      }
      .tap(result => changes.offer(SetElement(name, value)))

    override def deleteElement(name: String): Task[Unit] = ref.update { el =>
      el.removed(name)
    } *> changes.offer(DeleteElement(name)).unit

  })
}

object CRef {

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

  def lock[R, A, T](id: IdProvider = AutoId)(f: => RIO[R, T])(implicit trace: Trace): RIO[CRefContext & R, T] =
    for {
      context    <- ZIO.service[CRefContext]
      stack      <- ZIO.stackTrace
      hash       <- sha(stack.prettyPrint)
      traceInfo  <-
        ZIO.fromOption(instance.unapply(trace)).orElseFail(new Throwable("No trace available"))
      a          <- Random.nextLong
      bytes      <- fromEither(serializeToArray(a)).mapError(failure => new Throwable(failure.message))
      location    = traceInfo._1
      file        = traceInfo._2
      line        = traceInfo._3
      name        = id match {
                      case AutoId       => s"$location:$file:$line ($hash)"
                      case ManualId(id) => id
                    }
      gainedLock <- context.setElementIfNotExist(name, bytes)

      interruptStream      <- Promise.make[Throwable, Unit]
      aliveInterruptStream <- Promise.make[Throwable, Unit]
      aliveFiber           <- context.keepAliveStream(name).interruptWhen(aliveInterruptStream).runDrain.fork
      _                    <- context // attempt to gain lock when delete element happen, if not keep listening otherwise terminate stream
                                .onChangeStream(name)
                                .interruptWhen(interruptStream)
                                .collectZIO { case DeleteElement(name) =>
                                  for {
                                    lockedGain <- context.setElementIfNotExist(name, bytes)
                                    _          <- interruptStream.succeed(()).when(lockedGain)
                                  } yield ()
                                }
                                .runDrain
                                .unless(gainedLock)
      result               <- f
      _                    <- aliveInterruptStream.succeed(())
      _                    <- context.deleteElement(name)
    } yield result

  def make[T: BinaryCodec](a: => T, id: IdProvider = AutoId)(implicit trace: Trace): RIO[CRefContext, CRef[T]] =
    for {
      context   <- ZIO.service[CRefContext]
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
      _         <- context.setElement(name, bytes)
    } yield new CRefImpl[T](context)(name)
}

sealed trait IdProvider

case object AutoId extends IdProvider
case class ManualId(value: String) extends IdProvider

class CRefImpl[T: BinaryCodec](context: CRefContext)(name: String) extends CRef[T] {

  override def get: RIO[CRefContext, T] = {
    val result: Task[Option[Array[Byte]]] = context.getElement(name)
    result.flatMap {
      case Some(bytes) =>
        fromEither(deserializeFromArray[T](bytes)).mapError(failure => new Throwable(failure.message))
      case None        => ZIO.fail(new Throwable(s"Element $name not found"))
    }
  }

  override def set(a: T): RIO[CRefContext, Unit] = fromEither(serializeToArray(a))
    .mapError(failure => new Throwable(failure.message))
    .flatMap(context.setElement(name, _))

  override def onChange[R](a: T => Task[R]): RIO[CRefContext, Unit] = changeStream.foreach(a).fork.unit

  override def changeStream: ZStream[CRefContext, Throwable, T] =
    context.onChangeStream(name).collectZIO { case SetElement(_, elementValue) =>
      val value = deserializeFromArray[T](elementValue)
      fromEither(value).mapError(failure => new Throwable(failure.message))
    }

  override def setIfNotExist(a: T): RIO[CRefContext, Boolean] =
    fromEither(serializeToArray(a))
      .mapError(failure => new Throwable(failure.message))
      .flatMap(context.setElementIfNotExist(name, _))

}

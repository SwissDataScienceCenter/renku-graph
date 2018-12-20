package ch.datascience.interpreters

import cats.Monad
import io.chrisdavenport.log4cats.Logger
import org.scalatest.Matchers._

import scala.collection.mutable.ArrayBuffer
import scala.language.higherKinds

class TestLogger[Interpretation[_] : Monad] extends Logger[Interpretation] {

  import TestLogger.Level._
  import TestLogger._

  def loggedOnly(level: Level, message: String): Unit =
    invocations should contain only (level -> Message(message))

  def expectNoLogs(): Unit =
    if (invocations.nonEmpty) fail(s"No logs expected but got $invocationsPrettyPrint")

  override def error(t: Throwable)(message: => String): Interpretation[Unit] = {
    invocations += (Error -> MessageAndThrowable(message, t))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def warn(t: Throwable)(message: => String): Interpretation[Unit] = {
    invocations += (Warn -> MessageAndThrowable(message, t))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def info(t: Throwable)(message: => String): Interpretation[Unit] = {
    invocations += (Info -> MessageAndThrowable(message, t))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def debug(t: Throwable)(message: => String): Interpretation[Unit] = {
    invocations += (Debug -> MessageAndThrowable(message, t))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def trace(t: Throwable)(message: => String): Interpretation[Unit] = {
    invocations += (Trace -> MessageAndThrowable(message, t))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def error(message: => String): Interpretation[Unit] = {
    invocations += (Error -> Message(message))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def warn(message: => String): Interpretation[Unit] = {
    invocations += (Warn -> Message(message))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def info(message: => String): Interpretation[Unit] = {
    invocations += (Info -> Message(message))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def debug(message: => String): Interpretation[Unit] = {
    invocations += (Debug -> Message(message))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def trace(message: => String): Interpretation[Unit] = {
    invocations += (Trace -> Message(message))
    implicitly[Monad[Interpretation]].pure(())
  }

  private[this] val invocations = ArrayBuffer.empty[(Level, Payload)]

  private val invocationsPrettyPrint: () => String = () =>
    invocations
      .map {
        case (level, Message(message))                        => s"$level '$message'"
        case (level, MessageAndThrowable(message, throwable)) => s"$level '$message' $throwable"
      }
      .mkString("\n", "\n", "")
}

object TestLogger {

  def apply[Interpretation[_]: Monad](): TestLogger[Interpretation] = new TestLogger[Interpretation]

  sealed trait Level
  object Level {
    case object Error extends Level
    case object Warn extends Level
    case object Info extends Level
    case object Debug extends Level
    case object Trace extends Level
  }

  private sealed trait Payload
  private case class Message(message: String) extends Payload
  private case class MessageAndThrowable(message: String, throwable: Throwable) extends Payload
}
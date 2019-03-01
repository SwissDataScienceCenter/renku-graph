/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.interpreters

import cats.Monad
import ch.datascience.interpreters.TestLogger.LogMessage._
import io.chrisdavenport.log4cats.Logger
import org.scalatest.Matchers._

import scala.collection.mutable.ArrayBuffer
import scala.language.higherKinds

class TestLogger[Interpretation[_]: Monad] extends Logger[Interpretation] {

  import TestLogger.Level._
  import TestLogger._
  import LogMessage._

  private[this] val invocations = ArrayBuffer.empty[LogEntry]

  def logged(expected: LogEntry*): Unit =
    invocations should contain allElementsOf expected

  def notLogged(expected: LogEntry): Unit =
    invocations should not contain expected

  def loggedOnly(expected: LogEntry*): Unit =
    loggedOnly(expected.toList)

  def loggedOnly(expected: List[LogEntry]): Unit =
    invocations should contain only (expected: _*)

  def expectNoLogs(): Unit =
    if (invocations.nonEmpty) fail(s"No logs expected but got $invocationsPrettyPrint")

  override def error(t: Throwable)(message: => String): Interpretation[Unit] = {
    invocations += LogEntry(Error, MessageAndThrowable(message, t))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def warn(t: Throwable)(message: => String): Interpretation[Unit] = {
    invocations += LogEntry(Warn, MessageAndThrowable(message, t))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def info(t: Throwable)(message: => String): Interpretation[Unit] = {
    invocations += LogEntry(Info, MessageAndThrowable(message, t))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def debug(t: Throwable)(message: => String): Interpretation[Unit] = {
    invocations += LogEntry(Debug, MessageAndThrowable(message, t))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def trace(t: Throwable)(message: => String): Interpretation[Unit] = {
    invocations += LogEntry(Trace, MessageAndThrowable(message, t))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def error(message: => String): Interpretation[Unit] = {
    invocations += LogEntry(Error, Message(message))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def warn(message: => String): Interpretation[Unit] = {
    invocations += LogEntry(Warn, Message(message))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def info(message: => String): Interpretation[Unit] = {
    invocations += LogEntry(Info, Message(message))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def debug(message: => String): Interpretation[Unit] = {
    invocations += LogEntry(Debug, Message(message))
    implicitly[Monad[Interpretation]].pure(())
  }

  override def trace(message: => String): Interpretation[Unit] = {
    invocations += LogEntry(Trace, Message(message))
    implicitly[Monad[Interpretation]].pure(())
  }

  private def invocationsPrettyPrint: String =
    invocations
      .map {
        case LogEntry(level, Message(message))                        => s"$level '$message'"
        case LogEntry(level, MessageAndThrowable(message, throwable)) => s"$level '$message' $throwable"
        case LogEntry(level, MessageAndThrowableMatcher(message, throwableMatcher)) =>
          s"$level '$message' $throwableMatcher"
      }
      .mkString("\n", "\n", "")
}

object TestLogger {

  def apply[Interpretation[_]: Monad](): TestLogger[Interpretation] = new TestLogger[Interpretation]

  private[TestLogger] case class LogEntry(level: Level, message: LogMessage)

  sealed trait Level {

    def apply(message: String): LogEntry =
      LogEntry(this, Message(message))

    def apply(message: String, throwable: Throwable): LogEntry =
      LogEntry(this, MessageAndThrowable(message, throwable))

    def apply(message: String, throwableMatcher: Matcher): LogEntry =
      LogEntry(this, MessageAndThrowableMatcher(message, throwableMatcher))
  }

  object Level {
    final case object Error extends Level
    final case object Warn  extends Level
    final case object Info  extends Level
    final case object Debug extends Level
    final case object Trace extends Level
  }

  sealed trait LogMessage
  object LogMessage {
    final case class Message(message:                    String) extends LogMessage
    final case class MessageAndThrowableMatcher(message: String, throwableMatcher: Matcher) extends LogMessage
    final case class MessageAndThrowable(message:        String, throwable: Throwable) extends LogMessage {
      override def equals(other: Any): Boolean = other match {
        case MessageAndThrowable(otherMessage, otherThrowable) =>
          (message == otherMessage) && (throwable == otherThrowable)
        case MessageAndThrowableMatcher(otherMessage, otherMatcher) =>
          (message == otherMessage) && (otherMatcher matches throwable)
        case _ => false
      }
    }
  }

  sealed trait Matcher {
    def matches(other: Throwable): Boolean
  }

  object Matcher {

    final case class NotRefEqual(throwable: Throwable) extends Matcher {

      override def matches(other: Throwable): Boolean =
        (other.getClass == throwable.getClass) &&
          (other.getMessage == throwable.getMessage)

      override lazy val toString: String = throwable.toString
    }
  }
}

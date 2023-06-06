/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.interpreters

import cats.Show
import cats.effect.kernel.Sync
import cats.syntax.all._
import io.renku.interpreters.TestLogger.LogMessage._
import org.scalatest.Assertion
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

class TestLogger[F[_]: Sync] extends Logger[F] with should.Matchers {

  import TestLogger.Level._
  import TestLogger._
  import LogMessage._

  private[this] val F           = Sync[F]
  private[this] val invocations = new ConcurrentLinkedQueue[LogEntry]()

  def getMessages(severity: Level): List[LogMessage] =
    invocations(of = severity).toList.map(_.message)

  private def invocations(of: Level): Iterable[LogEntry] =
    invocations.asScala.filter(_.level === of)

  def logged(expected: LogEntry*): Assertion =
    invocations should contain allElementsOf expected

  def notLogged(expected: LogEntry): Assertion =
    invocations should not contain expected

  def loggedOnly(expected: LogEntry*): Assertion =
    loggedOnly(expected.toList)

  def loggedOnly(expected: LogEntry, times: Int): Assertion =
    loggedOnly(List.fill(times)(expected))

  def loggedOnly(expected: List[LogEntry]): Assertion =
    invocations.asScala.toList should contain theSameElementsAs expected

  def expectNoLogs(): Unit =
    if (!invocations.isEmpty) fail(s"No logs expected but got $invocationsPrettyPrint")

  def expectNoLogs(severity: Level): Unit = invocations(of = severity).toList match {
    case Nil         => ()
    case invocations => fail(s"No $severity logs expected but got ${prettyPrint(invocations)}")
  }

  def reset(): Unit = invocations.clear()

  private def add(entry: LogEntry): F[Unit] = F.delay {
    invocations.add(entry)
    ()
  }

  override def error(t: Throwable)(message: => String): F[Unit] =
    add(LogEntry(Error, MessageAndThrowable(message, t)))

  override def warn(t: Throwable)(message: => String): F[Unit] =
    add(LogEntry(Warn, MessageAndThrowable(message, t)))

  override def info(t: Throwable)(message: => String): F[Unit] =
    add(LogEntry(Info, MessageAndThrowable(message, t)))

  override def debug(t: Throwable)(message: => String): F[Unit] =
    add(LogEntry(Debug, MessageAndThrowable(message, t)))

  override def trace(t: Throwable)(message: => String): F[Unit] =
    add(LogEntry(Trace, MessageAndThrowable(message, t)))

  override def error(message: => String): F[Unit] =
    add(LogEntry(Error, Message(message)))

  override def warn(message: => String): F[Unit] =
    add(LogEntry(Warn, Message(message)))

  override def info(message: => String): F[Unit] =
    add(LogEntry(Info, Message(message)))

  override def debug(message: => String): F[Unit] =
    add(LogEntry(Debug, Message(message)))

  override def trace(message: => String): F[Unit] =
    add(LogEntry(Trace, Message(message)))

  private def invocationsPrettyPrint: String = prettyPrint(invocations.asScala)

  private def prettyPrint(invocations: Iterable[LogEntry]): String = invocations
    .map {
      case LogEntry(level, Message(message))                        => s"$level '$message'"
      case LogEntry(level, MessageAndThrowable(message, throwable)) => s"$level '$message' $throwable"
      case LogEntry(level, MessageAndThrowableMatcher(message, throwableMatcher)) =>
        s"$level '$message' $throwableMatcher"
    }
    .mkString("\n", "\n", "")
}

object TestLogger {

  def apply[F[_]: Sync](): TestLogger[F] = new TestLogger[F]

  private[TestLogger] case class LogEntry(level: Level, message: LogMessage) {
    override lazy val toString = show"\n\t$level: $message"
  }

  sealed trait Level extends Product with Serializable {

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

    implicit lazy val show: Show[Level] = Show.show(_.productPrefix.toUpperCase)
  }

  sealed trait LogMessage {
    val message: String
  }
  object LogMessage {
    final case class Message(message: String)                                               extends LogMessage
    final case class MessageAndThrowableMatcher(message: String, throwableMatcher: Matcher) extends LogMessage
    final case class MessageAndThrowable(message: String, throwable: Throwable) extends LogMessage {
      override def equals(other: Any): Boolean = other match {
        case MessageAndThrowable(otherMessage, otherThrowable) =>
          (message == otherMessage) && (throwable == otherThrowable)
        case MessageAndThrowableMatcher(otherMessage, otherMatcher) =>
          (message == otherMessage) && (otherMatcher matches throwable)
        case _ => false
      }
    }

    implicit lazy val show: Show[LogMessage] = Show.show {
      case Message(m)                        => m
      case MessageAndThrowableMatcher(m, _)  => m
      case MessageAndThrowable(m, throwable) => s"$m; ${throwable.getClass.getName}: ${throwable.getMessage}"
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

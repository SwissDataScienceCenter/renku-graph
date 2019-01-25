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
import ch.datascience.interpreters.TestLogger.Matcher.NotRefEqual
import io.chrisdavenport.log4cats.Logger
import org.scalatest.Matchers._

import scala.collection.mutable.ArrayBuffer
import scala.language.higherKinds

class TestLogger[Interpretation[_]: Monad] extends Logger[Interpretation] {

  import TestLogger.Level._
  import TestLogger._
  import LogMessage._

  def loggedOnly(args: (Level, LogMessage)*): Unit =
    loggedOnly(args.toList)

  def loggedOnly(args: List[(Level, LogMessage)]): Unit =
    (invocations zip args) foreach {
      case ((actualLevel, MessageAndThrowable(actualMessage, actualException)),
            (expectedLevel, MessageAndThrowableMatcher(expectedMessage, NotRefEqual(expectedException)))) =>
        if ((actualLevel == expectedLevel) &&
            (actualMessage == expectedMessage) &&
            (actualException.getClass == expectedException.getClass) &&
            (actualException.getMessage == expectedException.getMessage)) ()
        else
          fail {
            s"Log entry: '$actualLevel: $actualMessage: $actualException' does not match '$expectedLevel: $expectedMessage: $expectedException'"
          }
      case ((actualLevel, MessageAndThrowable(actualMessage, actualException)),
            (expectedLevel, MessageAndThrowable(expectedMessage, expectedException))) =>
        if ((actualLevel == expectedLevel) &&
            (actualMessage == expectedMessage) &&
            (actualException == expectedException)) ()
        else
          fail {
            s"Log entry: '$actualLevel: $actualMessage: $actualException' does not match '$expectedLevel: $expectedMessage: $expectedException'"
          }
      case ((actualLevel, Message(actualMessage)), (expectedLevel, Message(expectedMessage))) =>
        if ((actualLevel == expectedLevel) &&
            (actualMessage == expectedMessage)) ()
        else
          fail {
            s"Log entry: '$actualLevel: $actualMessage' does not match '$expectedLevel: $expectedMessage'"
          }
      case ((actualLevel, actualMessage), (expectedLevel, expectedMessage)) =>
        if ((actualLevel == expectedLevel) &&
            (actualMessage == expectedMessage)) ()
        else
          fail {
            s"Log entry: '$actualLevel: $actualMessage' does not match '$expectedLevel: $expectedMessage'"
          }
    }

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

  private[this] val invocations = ArrayBuffer.empty[(Level, LogMessage)]

  private val invocationsPrettyPrint: () => String = () =>
    invocations
      .map {
        case (level, Message(message))                                      => s"$level '$message'"
        case (level, MessageAndThrowable(message, throwable))               => s"$level '$message' $throwable"
        case (level, MessageAndThrowableMatcher(message, throwableMatcher)) => s"$level '$message' $throwableMatcher"
      }
      .mkString("\n", "\n", "")
}

object TestLogger {

  def apply[Interpretation[_]: Monad](): TestLogger[Interpretation] = new TestLogger[Interpretation]

  sealed trait Level {

    def apply(message: String): (Level, LogMessage) =
      this -> Message(message)

    def apply(message: String, throwable: Throwable): (Level, LogMessage) =
      this -> MessageAndThrowable(message, throwable)

    def apply(message: String, throwableMatcher: Matcher): (Level, LogMessage) =
      this -> MessageAndThrowableMatcher(message, throwableMatcher)
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
    final case class MessageAndThrowable(message:        String, throwable: Throwable) extends LogMessage
    final case class MessageAndThrowableMatcher(message: String, throwableMatcher: Matcher) extends LogMessage
  }

  sealed trait Matcher
  object Matcher {
    final case class NotRefEqual(throwable: Throwable) extends Matcher {
      override lazy val toString: String = throwable.toString
    }
  }
}

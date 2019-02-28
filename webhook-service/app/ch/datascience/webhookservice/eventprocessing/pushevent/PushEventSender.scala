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

package ch.datascience.webhookservice.eventprocessing.pushevent

import cats.effect.IO
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.graph.model.events.CommitEvent
import ch.datascience.logging.IOLogger
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.eventprocessing.commitevent.{CommitEventSender, IOCommitEventSender}
import io.chrisdavenport.log4cats.Logger
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.control.NonFatal

class PushEventSender[Interpretation[_]: Monad](
    commitEventsFinder: CommitEventsFinder[Interpretation],
    commitEventSender:  CommitEventSender[Interpretation],
    logger:             Logger[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import commitEventSender._
  import commitEventsFinder._

  def storeCommitsInEventLog(pushEvent: PushEvent): Interpretation[Unit] = {
    for {
      commitEventsStream <- findCommitEvents(pushEvent)
      _                  <- (commitEventsStream map sendEvent(pushEvent)).sequence
      _                  <- logger.info(logMessageFor(pushEvent, "stored in event log"))
    } yield ()
  } recoverWith loggingError(pushEvent)

  private def sendEvent(pushEvent: PushEvent)(maybeCommitEvent: Interpretation[CommitEvent]) = {
    for {
      commitEvent <- maybeCommitEvent recoverWith fetchErrorLogging
      _           <- send(commitEvent) recoverWith sendErrorLogging(commitEvent)
      _           <- logger.info(logMessageFor(pushEvent, "stored in event log", Some(commitEvent)))
    } yield ()
  } recover withLogging(pushEvent)

  private def loggingError(pushEvent: PushEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(logMessageFor(pushEvent, "storing in event log failed"))
      ME.raiseError(exception)
  }

  private case class CommitEventProcessingException(
      maybeCommitEvent: Option[CommitEvent],
      message:          String,
      cause:            Throwable
  ) extends Exception

  private lazy val fetchErrorLogging: PartialFunction[Throwable, Interpretation[CommitEvent]] = {
    case NonFatal(exception) =>
      ME.raiseError(
        CommitEventProcessingException(
          maybeCommitEvent = None,
          message          = "fetching one of the commit events failed",
          cause            = exception
        )
      )
  }

  private def sendErrorLogging(commitEvent: CommitEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      ME.raiseError(
        CommitEventProcessingException(
          maybeCommitEvent = Some(commitEvent),
          message          = "storing in event log failed",
          cause            = exception
        )
      )
  }

  private def withLogging(pushEvent: PushEvent): PartialFunction[Throwable, Unit] = {
    case CommitEventProcessingException(maybeCommitEvent, message, cause) =>
      logger.error(cause)(logMessageFor(pushEvent, message, maybeCommitEvent))
    case NonFatal(exception) =>
      ME.raiseError(exception)
  }

  private def logMessageFor(
      pushEvent:        PushEvent,
      message:          String,
      maybeCommitEvent: Option[CommitEvent] = None
  ) =
    s"PushEvent commitTo: ${pushEvent.commitTo}, " +
      s"project: ${pushEvent.project.id}" +
      s"${maybeCommitEvent.map(_.id).map(id => s", CommitEvent id: $id").getOrElse("")}" +
      s": $message"

}

@Singleton
class IOPushEventSender @Inject()(
    commitEventFinder: IOCommitEventsFinder,
    commitEventSender: IOCommitEventSender,
    logger:            IOLogger
) extends PushEventSender[IO](commitEventFinder, commitEventSender, logger)

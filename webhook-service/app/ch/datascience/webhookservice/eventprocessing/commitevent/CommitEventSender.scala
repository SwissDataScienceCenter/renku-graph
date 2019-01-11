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

package ch.datascience.webhookservice.eventprocessing.commitevent

import cats.effect.IO
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.graph.events.CommitEvent
import ch.datascience.logging.IOLogger
import io.chrisdavenport.log4cats.Logger
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.control.NonFatal

class CommitEventSender[Interpretation[_]: Monad](
    eventLog:              EventLog[Interpretation],
    commitEventSerializer: CommitEventSerializer[Interpretation],
    logger:                Logger[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable]) {

  import commitEventSerializer._

  def send(commitEvent: CommitEvent): Interpretation[Unit] = {
    for {
      serialisedEvent <- serialiseToJsonString(commitEvent)
      _               <- eventLog.append(serialisedEvent)
      _               <- logger.info(s"Commit event id: ${commitEvent.id}, project: ${commitEvent.project.id} stored")
    } yield ()
  } recoverWith loggingError(commitEvent)

  private def loggingError(commitEvent: CommitEvent): PartialFunction[Throwable, Interpretation[Unit]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Storing commit event id: ${commitEvent.id}, project: ${commitEvent.project.id} failed")
      ME.raiseError(exception)
  }
}

@Singleton
class IOCommitEventSender @Inject()(
    eventLog:              EventLog[IO],
    commitEventSerializer: IOCommitEventSerializer,
    logger:                IOLogger,
) extends CommitEventSender[IO](eventLog, commitEventSerializer, logger)

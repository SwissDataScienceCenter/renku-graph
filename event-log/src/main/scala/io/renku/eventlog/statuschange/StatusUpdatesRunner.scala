/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.statuschange

import cats.effect.{Bracket, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.statuschange.commands.UpdateResult.{NotFound, Updated}
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}

trait StatusUpdatesRunner[Interpretation[_]] {
  def run(command: ChangeStatusCommand[Interpretation]): Interpretation[UpdateResult]
}

class StatusUpdatesRunnerImpl(
    transactor:       DbTransactor[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
    logger:           Logger[IO]
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with StatusUpdatesRunner[IO] {

  private implicit val transact: DbTransactor[IO, EventLogDB] = transactor

  import doobie.implicits._

  override def run(command: ChangeStatusCommand[IO]): IO[UpdateResult] =
    checkIfPersisted(command.eventId) transact transactor.get flatMap {
      case true =>
        for {
          queryResult  <- measureExecutionTime(command.query) transact transactor.get
          updateResult <- ME.catchNonFatal(command mapResult queryResult)
          _            <- logInfo(command, updateResult)
          _            <- command updateGauges updateResult
        } yield updateResult
      case false => NotFound.pure[IO]
    }

  private def logInfo(command: ChangeStatusCommand[IO], updateResult: UpdateResult) = updateResult match {
    case Updated => logger.info(s"Event ${command.eventId} got ${command.status}")
    case _       => ME.unit
  }

  private def checkIfPersisted(eventId: CompoundEventId) = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT event_id
            |FROM event
            |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId}""".stripMargin
        .query[String]
        .option
        .map(_.isDefined),
      name = "Event update check existence"
    )
  }
}

object IOUpdateCommandsRunner {

  import cats.effect.IO

  def apply(transactor:       DbTransactor[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
            logger:           Logger[IO]
  ): IO[StatusUpdatesRunner[IO]] = IO {
    new StatusUpdatesRunnerImpl(transactor, queriesExecTimes, logger)
  }
}

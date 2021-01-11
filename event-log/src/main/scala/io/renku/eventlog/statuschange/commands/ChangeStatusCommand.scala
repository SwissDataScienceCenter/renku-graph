/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.statuschange.commands

import cats.data.NonEmptyList
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import doobie.free.connection.ConnectionIO
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.eventlog.{EventLogDB, TypesSerializers}

trait ChangeStatusCommand[Interpretation[_]] extends Product with Serializable with TypesSerializers {
  def eventId: CompoundEventId
  def status:  EventStatus
  def query:   SqlQuery[Int]
  def updateGauges(updateResult: UpdateResult)(implicit
      transactor:                DbTransactor[Interpretation, EventLogDB]
  ): Interpretation[Unit]

  def mapResult: Int => UpdateResult = {
    case 0 => UpdateResult.NotFound
    case 1 => UpdateResult.Updated
    case _ => UpdateResult.Conflict
  }

  def runUpdateQueriesIfSuccessful(queries: NonEmptyList[doobie.Update0]): ConnectionIO[Int] =
    (1, queries.toList)
      .iterateWhileM {
        case (_, query :: remainingQueries) =>
          query.run.map(result => (result, remainingQueries))
        case (previousResult, Nil) => (previousResult, List.empty[doobie.Update0]).pure[ConnectionIO]
      } { case (previsousResult, queries) => previsousResult == 1 && queries.nonEmpty }
      .map(_._1)

}

sealed trait UpdateResult extends Product with Serializable

object UpdateResult {
  case object Conflict extends UpdateResult
  case object Updated  extends UpdateResult
  case object NotFound extends UpdateResult
  case class Failure(message: String Refined NonEmpty) extends UpdateResult
}

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

package io.renku.eventlog.events.categories.statuschange

import cats.data.Kleisli
import cats.effect.{BracketThrow, IO}
import ch.datascience.db.{DbClient, SqlStatement}
import ch.datascience.graph.model.events.{CompoundEventId, EventId}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.TypeSerializers
import skunk.Session
import skunk.data.Completion

private trait DeliveryInfoRemover[Interpretation[_]] {
  def deleteDelivery(eventId: CompoundEventId): Kleisli[Interpretation, Session[Interpretation], Unit]
}

private object DeliveryInfoRemover {
  def apply(queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name]): IO[DeliveryInfoRemover[IO]] = IO {
    new DeliveryInfoRemoverImpl(queriesExecTimes)
  }
}

private class DeliveryInfoRemoverImpl[Interpretation[_]: BracketThrow](
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name]
) extends DbClient(Some(queriesExecTimes))
    with DeliveryInfoRemover[Interpretation]
    with TypeSerializers {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import skunk._
  import skunk.implicits._

  override def deleteDelivery(eventId: CompoundEventId): Kleisli[Interpretation, Session[Interpretation], Unit] =
    measureExecutionTime {
      SqlStatement(name = "delivery info remove - status update")
        .command[EventId ~ projects.Id](
          sql"""DELETE FROM event_delivery
                WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
               """.command
        )
        .arguments(eventId.id ~ eventId.projectId)
        .build
        .flatMapResult {
          case Completion.Delete(_) => ().pure[Interpretation]
          case completion =>
            new Exception(s"Could not remove delivery info for ${eventId.show}: $completion")
              .raiseError[Interpretation, Unit]
        }
    }
}

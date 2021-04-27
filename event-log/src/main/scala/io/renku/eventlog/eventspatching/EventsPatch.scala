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

package io.renku.eventlog.eventspatching

import cats.MonadError
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import ch.datascience.db.SqlStatement
import ch.datascience.graph.model.events.EventStatus.New
import ch.datascience.graph.model.events.{BatchDate, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.eventlog.TypeSerializers
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private trait EventsPatch[Interpretation[_]] extends Product with Serializable with TypeSerializers {
  def name:               String Refined NonEmpty
  def updateGauges():     Interpretation[Unit]
  protected def sqlQuery: Kleisli[Interpretation, Session[Interpretation], Completion]
  lazy val query: SqlStatement[Interpretation, Completion] = SqlStatement(sqlQuery, name)
}

private case class StatusNewPatch[Interpretation[_]: Async: MonadError[*[_], Throwable]](
    waitingEventsGauge:   LabeledGauge[Interpretation, projects.Path],
    underProcessingGauge: LabeledGauge[Interpretation, projects.Path],
    now:                  () => Instant = () => Instant.now
) extends EventsPatch[Interpretation] {

  val status: EventStatus             = New
  val name:   String Refined NonEmpty = Refined.unsafeApply(s"status $status patch")

  protected override def sqlQuery: Kleisli[Interpretation, Session[Interpretation], Completion] = Kleisli { session =>
    val query: Command[EventStatus ~ BatchDate] = sql"""
      UPDATE event
      SET status = $eventStatusEncoder, execution_date = event_date, batch_date = $batchDateEncoder, message = NULL
    """.command
    session.prepare(query).use(_.execute(status ~ BatchDate(now())))
  }

  override def updateGauges(): Interpretation[Unit] =
    for {
      _ <- waitingEventsGauge.reset()
      _ <- underProcessingGauge.reset()
    } yield ()
}

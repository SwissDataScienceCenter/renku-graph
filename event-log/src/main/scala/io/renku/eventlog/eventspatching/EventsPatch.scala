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

package io.renku.eventlog.eventspatching

import java.time.Instant

import cats.MonadError
import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.eventlog.EventStatus.New
import io.renku.eventlog.{EventStatus, TypesSerializers}

import scala.language.higherKinds

private trait EventsPatch[Interpretation[_]] extends Product with Serializable with TypesSerializers {
  def name:               String Refined NonEmpty
  def updateGauges():     Interpretation[Unit]
  protected def sqlQuery: ConnectionIO[Int]
  lazy val query: SqlQuery[Int] = SqlQuery(sqlQuery, name)
}

private case class StatusNewPatch[Interpretation[_]](
    waitingEventsGauge:   LabeledGauge[Interpretation, projects.Path],
    underProcessingGauge: LabeledGauge[Interpretation, projects.Path],
    now:                  () => Instant = () => Instant.now
)(implicit ME:            MonadError[Interpretation, Throwable])
    extends EventsPatch[Interpretation] {

  val status: EventStatus             = New
  val name:   String Refined NonEmpty = Refined.unsafeApply(s"status $status patch")

  protected override def sqlQuery: ConnectionIO[Int] =
    sql"""|update event_log
          |set status = $status, execution_date = event_date, batch_date = ${now()}, message = NULL
          |""".stripMargin.update.run

  override def updateGauges(): Interpretation[Unit] =
    for {
      _ <- waitingEventsGauge.reset()
      _ <- underProcessingGauge.reset()
    } yield ()
}

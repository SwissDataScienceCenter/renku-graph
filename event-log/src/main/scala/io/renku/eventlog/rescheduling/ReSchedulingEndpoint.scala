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

package io.renku.eventlog.rescheduling

import cats.effect.{ContextShift, IO}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import io.chrisdavenport.log4cats.Logger
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import scala.language.higherKinds

trait ReSchedulingEndpoint[Interpretation[_]] {
  def triggerReScheduling: Interpretation[Response[Interpretation]]
}

class ReSchedulingEndpointImpl(
    reScheduler:         ReScheduler[IO],
    logger:              Logger[IO]
)(implicit contextShift: ContextShift[IO])
    extends Http4sDsl[IO]
    with ReSchedulingEndpoint[IO] {

  import ch.datascience.controllers.InfoMessage
  import ch.datascience.controllers.InfoMessage._
  import org.http4s._
  import reScheduler._

  override def triggerReScheduling: IO[Response[IO]] =
    for {
      _      <- scheduleEventsForProcessing.start
      result <- Accepted(InfoMessage("Events re-scheduling triggered"))
    } yield result
}

object IOReSchedulingEndpoint {
  import ch.datascience.db.DbTransactor
  import io.renku.eventlog.EventLogDB

  def apply(
      transactor:           DbTransactor[IO, EventLogDB],
      waitingEventsGauge:   LabeledGauge[IO, projects.Path],
      underProcessingGauge: LabeledGauge[IO, projects.Path],
      logger:               Logger[IO]
  )(implicit contextShift:  ContextShift[IO]): IO[ReSchedulingEndpoint[IO]] = IO {
    new ReSchedulingEndpointImpl(new IOReScheduler(transactor, waitingEventsGauge, underProcessingGauge), logger)
  }
}

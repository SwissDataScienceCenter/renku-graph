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

package io.renku.eventlog.latestevents

import cats.MonadError
import cats.effect.Effect
import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.metrics.LabeledHistogram
import io.chrisdavenport.log4cats.Logger
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import scala.util.control.NonFatal

class LatestEventsEndpoint[Interpretation[_]: Effect](
    latestEventsFinder: LatestEventsFinder[Interpretation],
    logger:             Logger[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import LatestEventsFinder.IdProjectBody
  import ch.datascience.controllers.ErrorMessage
  import ch.datascience.controllers.ErrorMessage._
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import latestEventsFinder._
  import org.http4s.circe._

  def findLatestEvents(): Interpretation[Response[Interpretation]] = {
    for {
      latestEvents <- findAllLatestEvents()
      response     <- Ok(latestEvents.asJson)
    } yield response
  } recoverWith internalServerError

  private implicit lazy val projectEncoder: Encoder[IdProjectBody] = Encoder.instance[IdProjectBody] {
    case (id, project, body) =>
      json"""{
      "id":     ${id.value},
      "project": {
        "id":   ${project.id.value},
        "path": ${project.path.value}
      },
      "body":   ${body.value}
    }"""
  }

  private lazy val internalServerError: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage("Finding all projects latest events failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }
}

object IOLatestEventsEndpoint {
  import cats.effect.{ContextShift, IO}
  import ch.datascience.db.DbTransactor
  import io.renku.eventlog.EventLogDB

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO]): IO[LatestEventsEndpoint[IO]] =
    for {
      eventsFinder <- IOLatestEventsFinder(transactor, queriesExecTimes)
    } yield new LatestEventsEndpoint[IO](eventsFinder, logger)
}

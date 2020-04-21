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

package ch.datascience.eventlog.latestevents

import cats.MonadError
import cats.effect.Effect
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.http4s.Response
import org.http4s.dsl.Http4sDsl

import scala.language.higherKinds
import scala.util.control.NonFatal

class LatestEventsEndpoint[Interpretation[_]: Effect](
    latestEventsFinder: LatestEventsFinder[Interpretation],
    logger:             Logger[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import ch.datascience.controllers.ErrorMessage
  import ch.datascience.controllers.ErrorMessage._
  import ch.datascience.eventlog.latestevents.LatestEventsFinder.IdProjectBody
  import io.circe.Encoder
  import io.circe.literal._
  import io.circe.syntax._
  import latestEventsFinder._
  import org.http4s.circe._

  def findLatestEvents: Interpretation[Response[Interpretation]] = {
    for {
      latestEvents <- findAllLatestEvents
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
  import ch.datascience.eventlog.EventLogDB

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      logger:              Logger[IO]
  )(implicit contextShift: ContextShift[IO]): IO[LatestEventsEndpoint[IO]] = IO {
    new LatestEventsEndpoint[IO](new IOLatestEventsFinder(transactor), logger)
  }
}

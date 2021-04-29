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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.ConfigLoader
import ch.datascience.control.Throttler
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.tinytypes.constraints.Url
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.CommitEvent
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import com.typesafe.config.{Config, ConfigFactory}
import org.typelevel.log4cats.Logger
import io.circe.Json

import scala.concurrent.ExecutionContext

// This TriplesGenerator supposed to be used by the acceptance-tests only

private[events] object RemoteTriplesGenerator extends ConfigLoader[IO] {

  def apply(
      configuration: Config = ConfigFactory.load()
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[TriplesGenerator[IO]] =
    for {
      serviceUrl <- find[String]("services.triples-generator.url", configuration) flatMap (url =>
                      IO.fromEither(TriplesGenerationServiceUrl from url)
                    )
    } yield new RemoteTriplesGenerator(serviceUrl, ApplicationLogger)
}

private[awaitinggeneration] class RemoteTriplesGenerator(
    serviceUrl:              TriplesGenerationServiceUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient[RemoteTriplesGenerator](Throttler.noThrottling, logger)
    with TriplesGenerator[IO] {

  import cats.data.EitherT
  import cats.effect._
  import ch.datascience.http.client.RestClientError.UnauthorizedException
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._

  override def generateTriples(
      commitEvent:             CommitEvent
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[IO, ProcessingRecoverableError, JsonLDTriples] =
    EitherT {
      (for {
        uri           <- validateUri(s"$serviceUrl/projects/${commitEvent.project.id}/commits/${commitEvent.commitId}")
        triplesInJson <- send(request(GET, uri))(mapResponse)
        triples       <- IO.fromEither(JsonLDTriples from triplesInJson)
      } yield triples.asRight[ProcessingRecoverableError]).recoverWith { case UnauthorizedException =>
        GenerationRecoverableError("Unauthorized exception").asLeft[JsonLDTriples].pure[IO]
      }
    }

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Json]] = {
    case (Ok, _, response)    => response.as[Json]
    case (Unauthorized, _, _) => IO.raiseError(UnauthorizedException)
  }
}

class TriplesGenerationServiceUrl private (val value: String) extends AnyVal with StringTinyType

object TriplesGenerationServiceUrl
    extends TinyTypeFactory[TriplesGenerationServiceUrl](new TriplesGenerationServiceUrl(_))
    with Url

/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.awaitinggeneration.triplesgeneration

import cats.MonadThrow
import cats.effect.kernel.Async
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import io.renku.config.ConfigLoader
import io.renku.control.Throttler
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.parser._
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.tinytypes.constraints.Url
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import ProcessingRecoverableError._
import io.renku.http.RenkuEntityCodec
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.CommitEvent
import org.typelevel.log4cats.Logger

// This TriplesGenerator supposed to be used by the acceptance-tests only

private[events] object RemoteTriplesGenerator {
  import ConfigLoader._

  def apply[F[_]: Async: Logger](configuration: Config = ConfigFactory.load()): F[TriplesGenerator[F]] = for {
    serviceUrl <- find[F, String]("services.remote-triples-generator.url", configuration) flatMap (url =>
                    MonadThrow[F].fromEither(TriplesGenerationServiceUrl from url)
                  )
  } yield new RemoteTriplesGenerator(serviceUrl)
}

private[awaitinggeneration] class RemoteTriplesGenerator[F[_]: Async: Logger](
    serviceUrl: TriplesGenerationServiceUrl
) extends RestClient(Throttler.noThrottling)
    with TriplesGenerator[F]
    with RenkuEntityCodec {

  import cats.data.EitherT
  import io.renku.http.client.RestClientError.UnauthorizedException
  import org.http4s._
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized

  import org.http4s.dsl.io._

  override def generateTriples(
      event: CommitEvent
  )(implicit at: AccessToken): EitherT[F, ProcessingRecoverableError, JsonLD] = EitherT {
    {
      for {
        uri           <- validateUri(s"$serviceUrl/projects/${event.project.id}/commits/${event.commitId}")
        triplesInJson <- send(request(GET, uri))(mapResponse)
        triples       <- MonadThrow[F].fromEither(parse(triplesInJson))
      } yield triples.asRight[ProcessingRecoverableError]
    } recoverWith { case UnauthorizedException =>
      SilentRecoverableError("Unauthorized exception").asLeft[JsonLD].leftWiden[ProcessingRecoverableError].pure[F]
    }
  }

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Json]] = {
    case (Ok, _, response)    => response.as[Json]
    case (Unauthorized, _, _) => UnauthorizedException.raiseError[F, Json]
  }
}

class TriplesGenerationServiceUrl private (val value: String) extends AnyVal with StringTinyType

object TriplesGenerationServiceUrl
    extends TinyTypeFactory[TriplesGenerationServiceUrl](new TriplesGenerationServiceUrl(_))
    with Url[TriplesGenerationServiceUrl]

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

package io.renku.knowledgegraph.users.projects

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import io.renku.config.renku
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.{RenkuUrl, persons}
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.GitLabClient
import io.renku.http.rest.paging.{PagingHeaders, PagingRequest, PagingResponse}
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.users.projects.Endpoint._
import io.renku.knowledgegraph.users.projects.finder._
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.io.OptionalValidatingQueryParamDecoderMatcher
import org.http4s.{ParseFailure, QueryParamDecoder, QueryParameterValue, Request, Response}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait Endpoint[F[_]] {
  def `GET /users/:id/projects`(criteria: Criteria, request: Request[F]): F[Response[F]]
}

object Endpoint {

  import Criteria.Filters._
  import Criteria._

  final case class Criteria(userId:    persons.GitLabId,
                            filters:   Filters = Filters(),
                            paging:    PagingRequest = PagingRequest.default,
                            maybeUser: Option[AuthUser] = None
  )

  object Criteria {

    final case class Filters(state: ActivationState = ActivationState.All)
    object Filters {

      sealed trait ActivationState extends StringTinyType with Product with Serializable
      object ActivationState extends TinyTypeFactory[ActivationState](activationStateFactory) {

        val all: Set[ActivationState] = Set(Activated, NotActivated, All)

        final case object Activated extends ActivationState {
          override val value: String = "ACTIVATED"
        }
        final case object NotActivated extends ActivationState {
          override val value: String = "NOT_ACTIVATED"
        }
        final case object All extends ActivationState {
          override val value: String = "ALL"
        }

        private implicit val queryParameterDecoder: QueryParamDecoder[ActivationState] =
          (value: QueryParameterValue) =>
            ActivationState
              .from(value.value)
              .leftMap(_ => ParseFailure(s"'${activationState.parameterName}' parameter with invalid value", ""))
              .toValidatedNel

        object activationState extends OptionalValidatingQueryParamDecoderMatcher[ActivationState]("state") {
          val parameterName: String = "state"
        }
      }
    }

    private lazy val activationStateFactory: String => ActivationState = value =>
      ActivationState.all.find(_.value equalsIgnoreCase value).getOrElse {
        throw new IllegalArgumentException(s"'$value' unknown ActivationState")
      }
  }

  def apply[F[_]: Async: Parallel: GitLabClient: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] = for {
    projectsFinder <- ProjectsFinder[F]
    renkuUrl       <- RenkuUrlLoader()
    renkuApiUrl    <- renku.ApiUrl()
  } yield new EndpointImpl[F](projectsFinder, renkuUrl, renkuApiUrl)
}

private class EndpointImpl[F[_]: Async: Logger](projectsFinder: ProjectsFinder[F],
                                                renkuUrl:    RenkuUrl,
                                                renkuApiUrl: renku.ApiUrl
) extends Http4sDsl[F]
    with RenkuEntityCodec
    with Endpoint[F] {

  import eu.timepit.refined.auto._
  import io.circe.syntax._
  import io.renku.data.Message
  import org.http4s.{Header, Request, Response, Status}

  private implicit val rnkUrl:    RenkuUrl     = renkuUrl
  private implicit val rnkApiUrl: renku.ApiUrl = renkuApiUrl

  override def `GET /users/:id/projects`(criteria: Criteria, request: Request[F]): F[Response[F]] =
    projectsFinder.findProjects(criteria) map toHttpResponse(request) recoverWith httpResult

  private def toHttpResponse(request: Request[F])(response: PagingResponse[model.Project]): Response[F] = {
    val resourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
    Response[F](Status.Ok)
      .withEntity(response.results.asJson)
      .putHeaders(PagingHeaders.from(response)(resourceUrl, renku.ResourceUrl).toSeq.map(Header.ToRaw.rawToRaw): _*)
  }

  private lazy val httpResult: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val message = Message.Error("Finding user's projects failed")
    Logger[F].error(exception)(message.show) >> InternalServerError(message)
  }
}

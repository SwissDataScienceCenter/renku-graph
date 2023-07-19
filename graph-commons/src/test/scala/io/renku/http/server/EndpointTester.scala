/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.http.server

import cats.data.{Kleisli, OptionT}
import cats.effect.unsafe.IORuntime
import cats.effect.{Concurrent, IO, Resource}
import cats.syntax.all._
import eu.timepit.refined.api.RefType
import io.circe._
import io.renku.data.ErrorMessage
import io.renku.http.InfoMessage.InfoMessage
import io.renku.http.rest.Links
import io.renku.http.rest.Links.{Href, Rel}
import io.renku.http.server.security.model.{AuthUser, MaybeAuthUser}
import io.renku.json.JsonOps.JsonExt
import org.http4s._
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.headers.`Content-Type`
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.server.AuthMiddleware

object EndpointTester {

  implicit val jsonEntityDecoder:     EntityDecoder[IO, Json]       = jsonOf[IO, Json]
  implicit val jsonListEntityDecoder: EntityDecoder[IO, List[Json]] = jsonOf[IO, List[Json]]
  implicit val jsonEntityEncoder:     EntityEncoder[IO, Json]       = jsonEncoderOf[IO, Json]

  implicit val stringEntityDecoder: EntityDecoder[IO, String] = EntityDecoder.text

  implicit class ResourceEndpointOps(routes: Resource[IO, Kleisli[IO, Request[IO], Response[IO]]]) {

    def call(request: Request[IO])(implicit runtime: IORuntime) = new {

      private val runResponse: Response[IO] = routes.use(_.run(request)).unsafeRunSync()

      lazy val status:      Status                 = runResponse.status
      lazy val contentType: Option[`Content-Type`] = runResponse.contentType

      def body[T](implicit decoder: EntityDecoder[IO, T]): T = runResponse.as[T].unsafeRunSync()
    }
  }

  val notAvailableResponse: Response[IO] = Response(Status.ServiceUnavailable)

  implicit class RoutesOps(routes: HttpRoutes[IO]) {
    def or(response: Response[IO]): Kleisli[IO, Request[IO], Response[IO]] =
      Kleisli(a => routes.run(a).getOrElse(response))
  }

  implicit val errorMessageDecoder: Decoder[ErrorMessage] = Decoder.instance[ErrorMessage] { cur =>
    def failure(v: Any): Decoder.Result[ErrorMessage] =
      DecodingFailure(s"Cannot deserialize 'message': $v to ErrorMessage", Nil).asLeft[ErrorMessage]

    cur
      .downField("message")
      .as[Json]
      .flatMap { json =>
        json.fold[Decoder.Result[ErrorMessage]](failure(Json.Null),
                                                failure(_),
                                                failure(_),
                                                ErrorMessage(_).asRight[DecodingFailure],
                                                failure(_),
                                                _ => ErrorMessage(json).asRight[DecodingFailure]
        )
      }
  }

  implicit def errorMessageEntityDecoder[F[_]: Concurrent]: EntityDecoder[F, ErrorMessage] = jsonOf[F, ErrorMessage]

  implicit val infoMessageDecoder: Decoder[InfoMessage] = Decoder.instance[InfoMessage] {
    _.downField("message")
      .as[String]
      .flatMap(RefType.applyRef[InfoMessage](_))
      .leftMap(error => DecodingFailure(s"Cannot deserialize 'message' to InfoMessage: $error", Nil))
  }

  implicit def infoMessageEntityDecoder[F[_]: Concurrent]: EntityDecoder[F, InfoMessage] = jsonOf[F, InfoMessage]

  implicit class JsonOps(override val json: Json) extends JsonExt {
    import io.circe.Decoder
    import io.renku.http.rest.Links
    import io.renku.http.rest.Links._

    lazy val _links: Decoder.Result[Links] = json.hcursor.downField("_links").as[Links]
  }

  implicit class HCursorOps(cursor: ACursor) {
    import io.circe.Decoder
    import io.renku.http.rest.Links
    import io.renku.http.rest.Links._

    lazy val _links: Decoder.Result[Links] = cursor.downField("_links").as[Links]
  }

  implicit class LinksOps(maybeLinksJson: Decoder.Result[Links]) {
    def get(rel: Rel): Option[Href] =
      for {
        linksJson <- maybeLinksJson.toOption
        link      <- linksJson get rel
      } yield link.href
  }

  def givenAuthIfNeededMiddleware(returning: IO[MaybeAuthUser]): AuthMiddleware[IO, MaybeAuthUser] =
    AuthMiddleware {
      Kleisli.liftF(OptionT.liftF(returning))
    }

  def givenAuthAsUnauthorized: AuthMiddleware[IO, MaybeAuthUser] =
    AuthMiddleware.noSpider[IO, MaybeAuthUser](
      Kleisli.liftF(OptionT.none[IO, MaybeAuthUser]),
      AuthMiddleware.defaultAuthFailure[IO]
    )

  def givenAuthMiddleware(returning: OptionT[IO, AuthUser]): AuthMiddleware[IO, AuthUser] =
    AuthMiddleware {
      Kleisli liftF returning
    }

  def givenAuthFailing(): AuthMiddleware[IO, MaybeAuthUser] = AuthMiddleware {
    Kleisli(_ => OptionT.none)
  }

  implicit class RequestOps(request: Request[IO]) {

    def addParts(parts: Part[IO]*): IO[Request[IO]] =
      createMultiparts(parts: _*)
        .map(mltParts => request.withEntity(mltParts).withHeaders(mltParts.headers))

    private def createMultiparts(parts: Part[IO]*): IO[Multipart[IO]] = Multiparts
      .forSync[IO]
      .flatMap(_.multipart(Vector(parts: _*)))
  }
}

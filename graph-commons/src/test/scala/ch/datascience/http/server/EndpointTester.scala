/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.http.server

import cats.data.Kleisli
import cats.effect.{ContextShift, IO, Sync}
import cats.implicits._
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.http.rest.Links
import ch.datascience.http.rest.Links.{Href, Rel}
import eu.timepit.refined.api.RefType
import io.circe._
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.{EntityDecoder, EntityEncoder, HttpRoutes, Request, Response, Status}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

object EndpointTester {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  implicit val jsonEntityDecoder:     EntityDecoder[IO, Json]       = jsonOf[IO, Json]
  implicit val jsonListEntityDecoder: EntityDecoder[IO, List[Json]] = jsonOf[IO, List[Json]]
  implicit val jsonEntityEncoder:     EntityEncoder[IO, Json]       = jsonEncoderOf[IO, Json]

  implicit class EndpointOps(endpoint: Kleisli[IO, Request[IO], Response[IO]]) {

    def test(request: Request[IO]) = new {
      private val runResponse: Response[IO] = endpoint.run(request).unsafeRunSync()

      lazy val status: Status = runResponse.status

      def body[T](implicit decoder: EntityDecoder[IO, T]): T = runResponse.as[T].unsafeRunSync
    }
  }

  val notAvailableResponse: Response[IO] = Response(Status.ServiceUnavailable)

  implicit class RoutesOps(routes: HttpRoutes[IO]) {
    def or(response: Response[IO]): Kleisli[IO, Request[IO], Response[IO]] =
      Kleisli(a => routes.run(a).getOrElse(response))
  }

  implicit val errorMessageDecoder: Decoder[ErrorMessage] = Decoder.instance[ErrorMessage] {
    _.downField("message")
      .as[String]
      .flatMap(RefType.applyRef[ErrorMessage](_))
      .leftMap(error => DecodingFailure(s"Cannot deserialize 'message' to ErrorMessage: $error", Nil))
  }

  implicit def errorMessageEntityDecoder[F[_]: Sync]: EntityDecoder[F, ErrorMessage] = jsonOf[F, ErrorMessage]

  implicit class JsonOps(json: Json) {
    import ch.datascience.http.rest.Links
    import ch.datascience.http.rest.Links._
    import io.circe.Decoder

    lazy val _links: Decoder.Result[Links] = json.hcursor.downField("_links").as[Links]
  }

  implicit class HCursorOps(cursor: ACursor) {
    import ch.datascience.http.rest.Links
    import ch.datascience.http.rest.Links._
    import io.circe.Decoder

    lazy val _links: Decoder.Result[Links] = cursor.downField("_links").as[Links]
  }

  implicit class LinksOps(maybeLinksJson: Decoder.Result[Links]) {
    def get(rel: Rel): Option[Href] =
      for {
        linksJson <- maybeLinksJson.toOption
        link      <- linksJson get rel
      } yield link.href
  }
}

/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.tokenrepository

import cats.effect.Async
import cats.syntax.all._
import io.renku.control.Throttler
import io.renku.graph.model.projects.{Id, Path}
import io.renku.http.client.{AccessToken, RestClient}
import org.typelevel.log4cats.Logger

trait AccessTokenFinder[F[_]] extends AccessTokenFinder.Implicits {
  def findAccessToken[ID](projectId: ID)(implicit toPathSegment: ID => String): F[Option[AccessToken]]
}

class AccessTokenFinderImpl[F[_]: Async: Logger](
    tokenRepositoryUrl: TokenRepositoryUrl
) extends RestClient[F, AccessTokenFinder[F]](Throttler.noThrottling)
    with AccessTokenFinder[F] {

  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._

  def findAccessToken[ID](projectId: ID)(implicit toPathSegment: ID => String): F[Option[AccessToken]] =
    for {
      uri         <- validateUri(s"$tokenRepositoryUrl/projects/${toPathSegment(projectId)}/tokens")
      accessToken <- send(request(GET, uri))(mapResponse)
    } yield accessToken

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[AccessToken]]] = {
    case (Ok, _, response) => response.as[Option[AccessToken]]
    case (NotFound, _, _)  => Option.empty[AccessToken].pure[F]
  }

  private implicit lazy val accessTokenEntityDecoder: EntityDecoder[F, Option[AccessToken]] =
    jsonOf[F, AccessToken].map(Option.apply)
}

object AccessTokenFinder {

  trait Implicits {
    import io.renku.http.client.UrlEncoder.urlEncode
    implicit val projectPathToPath: Path => String = path => urlEncode(path.value)
    implicit val projectIdToPath:   Id => String   = _.toString
  }
  object Implicits extends Implicits

  def apply[F[_]](implicit ev: AccessTokenFinder[F]): AccessTokenFinder[F] = ev

  def apply[F[_]: Async: Logger](): F[AccessTokenFinder[F]] = for {
    tokenRepositoryUrl <- TokenRepositoryUrl[F]()
  } yield new AccessTokenFinderImpl[F](tokenRepositoryUrl)
}

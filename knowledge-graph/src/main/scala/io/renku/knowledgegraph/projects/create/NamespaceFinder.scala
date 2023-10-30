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

package io.renku.knowledgegraph.projects.create

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import org.http4s.Status.{Forbidden, NotFound, Ok}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.implicits._
import org.http4s.{Request, Response, Status}

private trait NamespaceFinder[F[_]] {
  def findNamespace(id: NamespaceId, at: AccessToken): F[Option[Namespace.WithName]]
}

private object NamespaceFinder {
  def apply[F[_]: Async: GitLabClient]: NamespaceFinder[F] = new NamespaceFinderImpl[F]
}

private class NamespaceFinderImpl[F[_]: Async: GitLabClient] extends NamespaceFinder[F] {

  override def findNamespace(id: NamespaceId, at: AccessToken): F[Option[Namespace.WithName]] =
    GitLabClient[F]
      .get(uri"namespaces" / id, "namespace-details")(mapResponse)(at.some)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[Namespace.WithName]]] = {
    case (Ok, _, resp)                => resp.as[Namespace.WithName].map(_.some)
    case (NotFound | Forbidden, _, _) => Option.empty[Namespace.WithName].pure[F]
  }

  private implicit lazy val decoder: Decoder[Namespace.WithName] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    Decoder.forProduct2("id", "full_path")(Namespace.WithName.apply)
  }
}

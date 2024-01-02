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

package io.renku.knowledgegraph.entities.currentuser.recentlyviewed

import cats.NonEmptyParallel
import cats.effect.kernel.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.config.renku
import io.renku.data.Message
import io.renku.entities.viewings.search.RecentEntitiesFinder
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.GitLabUrl
import io.renku.knowledgegraph.entities.ModelEncoders._
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.http4s.Response
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def getRecentlyViewedEntities(criteria: RecentEntitiesFinder.Criteria): F[Response[F]]
}

object Endpoint {
  def create[F[_]: Async: Logger](
      renkuApiUrl: renku.ApiUrl,
      gitLabUrl:   GitLabUrl,
      finder:      RecentEntitiesFinder[F]
  ): Endpoint[F] =
    new Impl[F](finder)(Async[F], Logger[F], renkuApiUrl, gitLabUrl)

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      finder: RecentEntitiesFinder[F]
  ): F[Endpoint[F]] = for {
    implicit0(renkuApiUrl: renku.ApiUrl) <- renku.ApiUrl()
    implicit0(gitLabUrl: GitLabUrl)      <- GitLabUrlLoader[F]()
  } yield new Impl[F](finder)

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): F[Endpoint[F]] =
    RecentEntitiesFinder[F](connectionConfig).flatMap(apply(_))

  private class Impl[F[_]: Async: Logger](finder: RecentEntitiesFinder[F])(implicit
      renkuApiUrl:  renku.ApiUrl,
      gitLabApiUrl: GitLabUrl
  ) extends Endpoint[F]
      with Http4sDsl[F] {

    override def getRecentlyViewedEntities(criteria: RecentEntitiesFinder.Criteria): F[Response[F]] =
      finder
        .findRecentlyViewedEntities(criteria)
        .flatMap(r => Ok(r.results))
        .recoverWith(ex =>
          Logger[F].error(ex)("Recent entity search failed!") *>
            InternalServerError(Message.Error("Recent entity search failed!"))
        )
  }
}

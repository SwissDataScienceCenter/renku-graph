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

package io.renku.graph.http.server.security

import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.graph.http.server.security.Authorizer.SecurityRecordFinder
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

object ProjectSlugRecordsFinder {
  def apply[F[_]: Async: Parallel: Logger: SparqlQueryTimeRecorder: GitLabClient]
      : F[SecurityRecordFinder[F, projects.Slug]] =
    (TSSlugRecordsFinder[F] -> GLSlugRecordsFinder[F])
      .mapN(new ProjectPathRecordsFinderImpl[F](_, _))
}

private class ProjectPathRecordsFinderImpl[F[_]: MonadThrow](tsPathRecordsFinder: TSSlugRecordsFinder[F],
                                                             glPathRecordsFinder: GLSlugRecordsFinder[F]
) extends SecurityRecordFinder[F, projects.Slug] {

  override def apply(slug: projects.Slug, maybeAuthUser: Option[AuthUser]): F[List[Authorizer.SecurityRecord]] =
    tsPathRecordsFinder(slug, maybeAuthUser) >>= {
      case Nil      => glPathRecordsFinder(slug, maybeAuthUser)
      case nonEmpty => nonEmpty.pure[F]
    }
}

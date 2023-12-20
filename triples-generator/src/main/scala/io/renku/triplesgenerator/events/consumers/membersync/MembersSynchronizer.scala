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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.projectauth.{ProjectAuthData, ProjectMember}
import io.renku.tokenrepository.api.TokenRepositoryClient
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait MembersSynchronizer[F[_]] {
  def synchronizeMembers(slug: projects.Slug): F[Unit]
}

private class MembersSynchronizerImpl[F[_]: MonadThrow: Logger](
    trClient:           TokenRepositoryClient[F],
    glMembersFinder:    GLProjectMembersFinder[F],
    glVisibilityFinder: GLProjectVisibilityFinder[F],
    projectAuthSync:    ProjectAuthSync[F]
) extends MembersSynchronizer[F] {

  override def synchronizeMembers(slug: projects.Slug): F[Unit] = {
    for {
      _                                   <- Logger[F].info(show"$categoryName: $slug accepted")
      implicit0(mat: Option[AccessToken]) <- trClient.findAccessToken(slug)
      membersInGL                         <- glMembersFinder.findProjectMembers(slug)
      maybeVisibility                     <- glVisibilityFinder.findVisibility(slug)
      _ <- maybeVisibility match {
             case None      => projectAuthSync.removeAuthData(slug)
             case Some(vis) => projectAuthSync.syncProject(ProjectAuthData(slug, toAuthMembers(membersInGL), vis))
           }

    } yield ()
  } handleErrorWith { exception =>
    Logger[F].error(exception)(s"$categoryName: Members synchronized for project $slug failed")
  }

  private lazy val toAuthMembers: Set[GitLabProjectMember] => Set[ProjectMember] =
    _.map(_.toProjectAuthMember)
}

private object MembersSynchronizer {
  def apply[F[_]: Async: GitLabClient: Logger: SparqlQueryTimeRecorder](
      projectSparqlClient: ProjectSparqlClient[F]
  ): F[MembersSynchronizer[F]] =
    for {
      implicit0(ru: RenkuUrl) <- RenkuUrlLoader[F]()
      trClient                <- TokenRepositoryClient[F]
    } yield new MembersSynchronizerImpl[F](trClient,
                                           GLProjectMembersFinder[F],
                                           GLProjectVisibilityFinder[F],
                                           ProjectAuthSync[F](projectSparqlClient)
    )
}

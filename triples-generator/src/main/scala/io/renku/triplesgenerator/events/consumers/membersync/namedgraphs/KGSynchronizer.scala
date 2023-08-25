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
package namedgraphs

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.triplesgenerator.events.consumers.ProjectAuthSync
import io.renku.triplesgenerator.gitlab.GitLabProjectMember
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private[membersync] object KGSynchronizer {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      projectSparqlClient: ProjectSparqlClient[F]
  ): F[KGSynchronizer[F]] =
    for {
      implicit0(renkuUrl: RenkuUrl) <- RenkuUrlLoader[F]()
      projectConnectionCfg          <- ProjectsConnectionConfig[F]()
      kgProjectMembersFinder = KGProjectMembersFinder[F](projectConnectionCfg, renkuUrl)
      kgPersonFinder         = KGPersonFinder[F](projectConnectionCfg)
      updatesCreator <- UpdatesCreator[F]
      tsClient        = TSClient[F](projectConnectionCfg)
      projectAuthSync = ProjectAuthSync[F](projectSparqlClient)
    } yield new KGSynchronizerImpl[F](kgProjectMembersFinder, kgPersonFinder, updatesCreator, projectAuthSync, tsClient)
}

private class KGSynchronizerImpl[F[_]: MonadThrow](
    kgMembersFinder: KGProjectMembersFinder[F],
    kgPersonFinder:  KGPersonFinder[F],
    updatesCreator:  UpdatesCreator,
    projectAuthSync: ProjectAuthSync[F],
    tsClient:        TSClient[F]
) extends KGSynchronizer[F] {
  import KGSynchronizerFunctions._

  override def syncMembers(slug: projects.Slug, membersInGL: Set[GitLabProjectMember]): F[SyncSummary] = for {
    membersInKG <- kgMembersFinder.findProjectMembers(slug)
    membersToAdd = findMembersToAdd(membersInGL, membersInKG)
    membersToAddWithIds <- kgPersonFinder.findPersonIds(membersToAdd)
    insertionUpdates = updatesCreator.insertion(slug, membersToAddWithIds)
    membersToRemove  = findMembersToRemove(membersInGL, membersInKG)
    removalUpdates   = updatesCreator.removal(slug, membersToRemove)
    _ <- (insertionUpdates ::: removalUpdates).map(tsClient.updateWithNoResult).sequence

    _ <- projectAuthSync.syncProject(slug, membersInGL.map(_.toProjectAuthMember))
  } yield SyncSummary(membersAdded = membersToAdd.size, membersRemoved = membersToRemove.size)
}

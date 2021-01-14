/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.membersync

import cats.MonadError
import cats.syntax.all._
import ch.datascience.graph.model.projects
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import io.chrisdavenport.log4cats.Logger

import scala.util.control.NonFatal

private trait MembersSynchronizer[Interpretation[_]] {
  def synchronizeMembers(projectPath: projects.Path): Interpretation[Unit]
}

private class MembersSynchronizerImpl[Interpretation[_]](
    accessTokenFinder:          AccessTokenFinder[Interpretation],
    gitLabProjectMembersFinder: GitLabProjectMembersFinder[Interpretation],
    kGProjectMembersFinder:     KGProjectMembersFinder[Interpretation],
    updatesCreator:             UpdatesCreator[Interpretation],
    querySender:                QuerySender[Interpretation],
    logger:                     Logger[Interpretation]
)(implicit ME:                  MonadError[Interpretation, Throwable])
    extends MembersSynchronizer[Interpretation] {
  import ch.datascience.graph.tokenrepository.IOAccessTokenFinder._

  override def synchronizeMembers(projectPath: projects.Path): Interpretation[Unit] = {
    for {
      maybeAccessToken <- accessTokenFinder.findAccessToken(projectPath)
      membersInGitLab  <- gitLabProjectMembersFinder.findProjectMembers(projectPath)(maybeAccessToken)
      membersInKG      <- kGProjectMembersFinder.findProjectMembers(projectPath)
      membersToAdd     = findMembersToAdd(membersInGitLab, membersInKG)
      membersToRemove  = findMembersToRemove(membersInGitLab, membersInKG)
      insertionUpdates = updatesCreator.insertion(projectPath, membersToAdd)
      removalUpdates   = updatesCreator.removal(projectPath, membersToRemove)
      _ <- (insertionUpdates ++ removalUpdates).map(querySender.send).sequence
      _ <- logger.info(s"${EventHandler.categoryName}: Members synchronized for project: $projectPath")
    } yield ()
  } recoverWith { case NonFatal(exception) =>
    logger.error(exception)(s"${EventHandler.categoryName}: Members synchronized for project $projectPath FAILED")
  }

  def findMembersToAdd(membersInGitLab: Set[GitLabProjectMember],
                       membersInKG:     Set[KGProjectMember]
  ): Set[GitLabProjectMember] = membersInGitLab.collect {
    case member @ GitLabProjectMember(gitlabId, _) if !membersInKG.exists(_.gitLabId == gitlabId) => member
  }

  def findMembersToRemove(membersInGitLab: Set[GitLabProjectMember],
                          membersInKG:     Set[KGProjectMember]
  ): Set[KGProjectMember] = membersInKG.collect {
    case member @ KGProjectMember(gitlabId) if !membersInGitLab.exists(_.id == gitlabId) => member
  }

}

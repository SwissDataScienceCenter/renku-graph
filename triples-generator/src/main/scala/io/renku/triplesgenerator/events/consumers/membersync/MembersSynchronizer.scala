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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait MembersSynchronizer[F[_]] {
  def synchronizeMembers(projectPath: projects.Path): F[Unit]
}

private class MembersSynchronizerImpl[F[_]: MonadThrow: AccessTokenFinder: Logger](
    gitLabProjectMembersFinder: GitLabProjectMembersFinder[F],
    kGProjectMembersFinder:     KGProjectMembersFinder[F],
    kGPersonFinder:             KGPersonFinder[F],
    updatesCreator:             UpdatesCreator,
    querySender:                QuerySender[F],
    executionTimeRecorder:      ExecutionTimeRecorder[F]
) extends MembersSynchronizer[F] {

  private val accessTokenFinder: AccessTokenFinder[F] = AccessTokenFinder[F]
  import accessTokenFinder._
  import executionTimeRecorder._

  override def synchronizeMembers(projectPath: projects.Path): F[Unit] = measureExecutionTime {
    findAccessToken(projectPath) >>= { implicit maybeAccessToken =>
      for {
        membersInGitLab <- gitLabProjectMembersFinder.findProjectMembers(projectPath)
        membersInKG     <- kGProjectMembersFinder.findProjectMembers(projectPath)
        membersToAdd = findMembersToAdd(membersInGitLab, membersInKG)
        membersToAddWithIds <- kGPersonFinder.findPersonIds(membersToAdd)
        insertionUpdates = updatesCreator.insertion(projectPath, membersToAddWithIds)
        membersToRemove  = findMembersToRemove(membersInGitLab, membersInKG)
        removalUpdates   = updatesCreator.removal(projectPath, membersToRemove)
        _ <- (insertionUpdates ::: removalUpdates).map(querySender.send).sequence
      } yield SyncSummary(projectPath, membersAdded = membersToAdd.size, membersRemoved = membersToRemove.size)
    }
  } flatMap logSummary recoverWith { case NonFatal(exception) =>
    Logger[F].error(exception)(s"$categoryName: Members synchronized for project $projectPath FAILED")
  }

  def findMembersToAdd(membersInGitLab: Set[GitLabProjectMember],
                       membersInKG:     Set[KGProjectMember]
  ): Set[GitLabProjectMember] = membersInGitLab.collect {
    case member @ GitLabProjectMember(gitlabId, _) if !membersInKG.exists(_.gitLabId == gitlabId) => member
  }

  def findMembersToRemove(membersInGitLab: Set[GitLabProjectMember],
                          membersInKG:     Set[KGProjectMember]
  ): Set[KGProjectMember] = membersInKG.collect {
    case member @ KGProjectMember(_, gitlabId) if !membersInGitLab.exists(_.gitLabId == gitlabId) => member
  }

  private case class SyncSummary(projectPath: projects.Path, membersAdded: Int, membersRemoved: Int)

  private def logSummary: ((ElapsedTime, SyncSummary)) => F[Unit] = {
    case (elapsedTime, SyncSummary(projectPath, membersAdded, membersRemoved)) =>
      Logger[F].info(
        s"$categoryName: Members for project: $projectPath synchronized in ${elapsedTime}ms: " +
          s"$membersAdded member(s) added, $membersRemoved member(s) removed"
      )
  }
}

private object MembersSynchronizer {
  def apply[F[_]: Async: GitLabClient: AccessTokenFinder: Logger: SparqlQueryTimeRecorder]: F[MembersSynchronizer[F]] =
    for {
      gitLabProjectMembersFinder <- GitLabProjectMembersFinder[F]
      kGProjectMembersFinder     <- KGProjectMembersFinder[F]
      kGPersonFinder             <- KGPersonFinder[F]
      updatesCreator             <- UpdatesCreator[F]
      rdfStoreConfig             <- RdfStoreConfig[F]()
      querySender <-
        MonadThrow[F].catchNonFatal(new RdfStoreClientImpl(rdfStoreConfig) with QuerySender[F] {
          override def send(query: SparqlQuery): F[Unit] = updateWithNoResult(query)
        })
      executionTimeRecorder <- ExecutionTimeRecorder[F](maybeHistogram = None)
    } yield new MembersSynchronizerImpl[F](gitLabProjectMembersFinder,
                                           kGProjectMembersFinder,
                                           kGPersonFinder,
                                           updatesCreator,
                                           querySender,
                                           executionTimeRecorder
    )
}

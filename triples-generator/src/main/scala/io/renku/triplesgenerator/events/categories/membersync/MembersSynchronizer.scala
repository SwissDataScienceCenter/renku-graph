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

package io.renku.triplesgenerator.events.categories.membersync

import cats.MonadThrow
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.projects
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.logging.ExecutionTimeRecorder.ElapsedTime
import ch.datascience.rdfstore._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private trait MembersSynchronizer[Interpretation[_]] {
  def synchronizeMembers(projectPath: projects.Path): Interpretation[Unit]
}

private class MembersSynchronizerImpl[Interpretation[_]: MonadThrow](
    accessTokenFinder:          AccessTokenFinder[Interpretation],
    gitLabProjectMembersFinder: GitLabProjectMembersFinder[Interpretation],
    kGProjectMembersFinder:     KGProjectMembersFinder[Interpretation],
    kGPersonFinder:             KGPersonFinder[Interpretation],
    updatesCreator:             UpdatesCreator,
    querySender:                QuerySender[Interpretation],
    logger:                     Logger[Interpretation],
    executionTimeRecorder:      ExecutionTimeRecorder[Interpretation]
) extends MembersSynchronizer[Interpretation] {

  import ch.datascience.graph.tokenrepository.AccessTokenFinder._
  import executionTimeRecorder._

  override def synchronizeMembers(projectPath: projects.Path): Interpretation[Unit] = measureExecutionTime {
    for {
      maybeAccessToken <- accessTokenFinder.findAccessToken(projectPath)
      membersInGitLab  <- gitLabProjectMembersFinder.findProjectMembers(projectPath)(maybeAccessToken)
      membersInKG      <- kGProjectMembersFinder.findProjectMembers(projectPath)
      membersToAdd = findMembersToAdd(membersInGitLab, membersInKG)
      membersToAddWithIds <- kGPersonFinder.findPersonIds(membersToAdd)
      insertionUpdates = updatesCreator.insertion(projectPath, membersToAddWithIds)
      membersToRemove  = findMembersToRemove(membersInGitLab, membersInKG)
      removalUpdates   = updatesCreator.removal(projectPath, membersToRemove)
      _ <- (insertionUpdates ::: removalUpdates).map(querySender.send).sequence
    } yield SyncSummary(projectPath, membersAdded = membersToAdd.size, membersRemoved = membersToRemove.size)
  } flatMap logSummary recoverWith { case NonFatal(exception) =>
    logger.error(exception)(s"$categoryName: Members synchronized for project $projectPath FAILED")
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

  private def logSummary: ((ElapsedTime, SyncSummary)) => Interpretation[Unit] = {
    case (elapsedTime, SyncSummary(projectPath, membersAdded, membersRemoved)) =>
      logger.info(
        s"$categoryName: Members for project: $projectPath synchronized in ${elapsedTime}ms: " +
          s"$membersAdded member(s) added, $membersRemoved member(s) removed"
      )
  }
}

private object MembersSynchronizer {
  def apply(gitLabThrottler: Throttler[IO, GitLab], timeRecorder: SparqlQueryTimeRecorder[IO])(implicit
      executionContext:      ExecutionContext,
      contextShift:          ContextShift[IO],
      timer:                 Timer[IO],
      logger:                Logger[IO]
  ): IO[MembersSynchronizer[IO]] = for {
    accessTokenFinder          <- AccessTokenFinder(logger)
    gitLabProjectMembersFinder <- IOGitLabProjectMembersFinder(gitLabThrottler, logger)
    kGProjectMembersFinder     <- KGProjectMembersFinder(logger, timeRecorder)
    kGPersonFinder             <- KGPersonFinder(timeRecorder)
    updatesCreator             <- UpdatesCreator()
    rdfStoreConfig             <- RdfStoreConfig[IO]()
    querySender <- IO(new RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder) with QuerySender[IO] {
                     override def send(query: SparqlQuery): IO[Unit] = updateWithNoResult(query)
                   })
    executionTimeRecorder <- ExecutionTimeRecorder[IO](logger, maybeHistogram = None)
  } yield new MembersSynchronizerImpl[IO](accessTokenFinder,
                                          gitLabProjectMembersFinder,
                                          kGProjectMembersFinder,
                                          kGPersonFinder,
                                          updatesCreator,
                                          querySender,
                                          logger,
                                          executionTimeRecorder
  )
}

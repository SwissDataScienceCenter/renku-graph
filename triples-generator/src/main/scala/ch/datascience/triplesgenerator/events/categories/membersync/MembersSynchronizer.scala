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
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.projects
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private trait MembersSynchronizer[Interpretation[_]] {
  def synchronizeMembers(projectPath: projects.Path): Interpretation[Unit]
}

private class MembersSynchronizerImpl[Interpretation[_]](
    accessTokenFinder:          AccessTokenFinder[Interpretation],
    gitLabProjectMembersFinder: GitLabProjectMembersFinder[Interpretation],
    kGProjectMembersFinder:     KGProjectMembersFinder[Interpretation],
    kGPersonFinder:             KGPersonFinder[Interpretation],
    updatesCreator:             UpdatesCreator,
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
      membersToAdd = findMembersToAdd(membersInGitLab, membersInKG)
      membersToAddWithIds <- kGPersonFinder.findPersonIds(membersToAdd)
      insertionUpdates = updatesCreator.insertion(projectPath, membersToAddWithIds)
      membersToRemove  = findMembersToRemove(membersInGitLab, membersInKG)
      removalUpdates   = updatesCreator.removal(projectPath, membersToRemove)
      _ <- (insertionUpdates :+ removalUpdates).map(querySender.send).sequence
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
    case member @ KGProjectMember(_, gitlabId) if !membersInGitLab.exists(_.gitLabId == gitlabId) => member
  }
}

private object MembersSynchronizer {
  def apply(gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO], timeRecorder: SparqlQueryTimeRecorder[IO])(
      implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[MembersSynchronizer[IO]] = for {
    accessTokenFinder          <- IOAccessTokenFinder(logger)
    gitLabProjectMembersFinder <- IOGitLabProjectMembersFinder(gitLabThrottler, logger)
    kGProjectMembersFinder     <- KGProjectMembersFinder(logger, timeRecorder)
    kGPersonFinder             <- KGPersonFinder(logger, timeRecorder)
    updatesCreator             <- UpdatesCreator()
    rdfStoreConfig             <- RdfStoreConfig[IO]()
    querySender <- IO(new IORdfStoreClient(rdfStoreConfig, logger, timeRecorder) with QuerySender[IO] {
                     override def send(query: SparqlQuery): IO[Unit] = updateWithNoResult(query)
                   })

  } yield new MembersSynchronizerImpl[IO](accessTokenFinder,
                                          gitLabProjectMembersFinder,
                                          kGProjectMembersFinder,
                                          kGPersonFinder,
                                          updatesCreator,
                                          querySender,
                                          logger
  )
}

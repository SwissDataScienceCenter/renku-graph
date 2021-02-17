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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration
package persondetails

import cats.{Monad, MonadError}
import cats.data.EitherT
import cats.effect.{ContextShift, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

private[triplescuration] trait PersonDetailsUpdater[Interpretation[_]] {
  def updatePersonDetails(curatedTriples: CuratedTriples[Interpretation],
                          projectPath:    projects.Path
  ): CurationResults[Interpretation]
}

private class PersonDetailsUpdaterImpl[Interpretation[_]: Monad](
    personExtractor:                 PersonExtractor[Interpretation],
    commitIdExtractor:               CommitIdExtractor[Interpretation],
    commitCommitterFinder:           CommitCommitterFinder[Interpretation],
    accessTokenFinder:               AccessTokenFinder[Interpretation],
    projectMembersFinder:            GitLabProjectMembersFinder[Interpretation],
    personsAndProjectMembersMatcher: PersonsAndProjectMembersMatcher,
    updatesCreator:                  UpdatesCreator
)(implicit ME:                       MonadError[Interpretation, Throwable])
    extends PersonDetailsUpdater[Interpretation] {

  import IOAccessTokenFinder._
  import accessTokenFinder._
  import personExtractor._
  import commitIdExtractor._
  import personsAndProjectMembersMatcher._
  import projectMembersFinder._
  import updatesCreator._

  def updatePersonDetails(curatedTriples: CuratedTriples[Interpretation],
                          projectPath:    projects.Path
  ): CurationResults[Interpretation] =
    for {
      triplesAndPersons <- getTriplesAndTrimmedPersons(curatedTriples.triples)
      (updatedTriples, trimmedPersons) = triplesAndPersons
      maybeAccessToken <- findAccessToken(projectPath)
      projectMembers   <- findProjectMembers(projectPath)(maybeAccessToken)
      personsWithGitlabIds = merge(trimmedPersons, projectMembers)
      newUpdatesGroups     = personsWithGitlabIds map prepareUpdates[Interpretation]
    } yield CuratedTriples(updatedTriples, curatedTriples.updatesGroups ++ newUpdatesGroups)

  private implicit class ResultOps[T](out: Interpretation[T]) {
    lazy val toRightT: EitherT[Interpretation, ProcessingRecoverableError, T] =
      EitherT.right[ProcessingRecoverableError](out)
  }

  private def getTriplesAndTrimmedPersons(triples: JsonLDTriples): Interpretation[(JsonLDTriples, Set[Person])] =
    for {
      triplesAndPersons <- extractPersons(triples).toRightT
      (triples, personDatas) = triplesAndPersons
      commitId       <- commitIdExtractor.extractCommitId(triples).toRightT
      trimmedPersons <- trimPersons(personDatas, commitId).toRightT
    } yield trimmedPersons

  private def trimPersons(personDatas: Set[PersonData], commitId: CommitId): Interpretation[Set[Person]] =
    personDatas.map {
      case (_, name :: names, _)     => tryToGetPersonFromGitLab(commitId)
      case (id, name :: Nil, emails) => Some(Person(id, name, emails.headOption))
    }.flatten

  // use extractCommitId

  // maybeEmail <- verify emails. if multiple, fail interpretation
  // if there is no email but there are multiple names, we can't do anything
  // verifyNames. if multiple, call gitlab to get single name for commit
  //                                commitInfoFinder (see other examples) will return a committer and author, both with name and email
  //                                try to match based on that response. it could be either but it should be committer
  //                                 if we can match, then we use it for the name in the person, else fail. failures defined in FailureOps
  //

  def tryToGetPersonFromGitLab(commitId: CommitId): Option[Person] = ???
}

private[triplescuration] object PersonDetailsUpdater {

  import cats.effect.IO

  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[PersonDetailsUpdater[IO]] = for {
    projectMembersFinder <- IOGitLabProjectMembersFinder(gitLabThrottler, logger)
    accessTokenFinder    <- IOAccessTokenFinder(logger)
    gitLabUrl            <- GitLabUrl[IO]()
  } yield new PersonDetailsUpdaterImpl[IO](
    PersonExtractor[IO](),
    IOCommitIdExtractor(),
    IOCommitCommitterFinder(),
    accessTokenFinder,
    projectMembersFinder,
    new PersonsAndProjectMembersMatcher(),
    new UpdatesCreator(gitLabUrl.apiV4)
  )
}

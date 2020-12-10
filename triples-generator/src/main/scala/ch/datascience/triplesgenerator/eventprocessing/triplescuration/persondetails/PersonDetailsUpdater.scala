/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration
package persondetails

import cats.MonadError
import cats.data.EitherT
import cats.effect.{ContextShift, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.Project
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

private[triplescuration] trait PersonDetailsUpdater[Interpretation[_]] {
  def updatePersonDetails(curatedTriples: CuratedTriples[Interpretation],
                          project:        Project
  ): CurationResults[Interpretation]
}

private class PersonDetailsUpdaterImpl[Interpretation[_]](
    personExtractor:                 PersonExtractor[Interpretation],
    accessTokenFinder:               AccessTokenFinder[Interpretation],
    projectMembersFinder:            GitLabProjectMembersFinder[Interpretation],
    personsAndProjectMembersMatcher: PersonsAndProjectMembersMatcher,
    updatesCreator:                  UpdatesCreator
)(implicit ME:                       MonadError[Interpretation, Throwable])
    extends PersonDetailsUpdater[Interpretation] {

  import IOAccessTokenFinder._
  import accessTokenFinder._
  import personExtractor._
  import personsAndProjectMembersMatcher._
  import projectMembersFinder._
  import updatesCreator._

  def updatePersonDetails(curatedTriples: CuratedTriples[Interpretation],
                          project:        Project
  ): CurationResults[Interpretation] =
    for {
      triplesAndPersons <- extractPersons(curatedTriples.triples).toRightT
      (updatedTriples, persons) = triplesAndPersons
      maybeAccessToken <- findAccessToken(project.path).toRightT
      projectMembers   <- findProjectMembers(project.path)(maybeAccessToken)
      personsWithGitlabIds = merge(persons, projectMembers)
      newUpdatesGroups     = personsWithGitlabIds map prepareUpdates[Interpretation]
    } yield CuratedTriples(updatedTriples, curatedTriples.updatesGroups ++ newUpdatesGroups)

  private implicit class ResultOps[T](out: Interpretation[T]) {
    lazy val toRightT: EitherT[Interpretation, ProcessingRecoverableError, T] =
      EitherT.right[ProcessingRecoverableError](out)
  }
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
    accessTokenFinder,
    projectMembersFinder,
    new PersonsAndProjectMembersMatcher(),
    new UpdatesCreator(gitLabUrl.apiV4)
  )
}

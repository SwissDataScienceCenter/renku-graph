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
import cats.effect.{ContextShift, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

private[triplescuration] trait PersonDetailsUpdater[Interpretation[_]] {
  def curate(curatedTriples: CuratedTriples[Interpretation]): Interpretation[CuratedTriples[Interpretation]]
}

private class PersonDetailsUpdaterImpl[Interpretation[_]](
    personExtractor:                 PersonExtractor[Interpretation],
    projectPathExtractor:            ProjectPathExtractor[Interpretation],
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
  import projectPathExtractor._
  import updatesCreator._

  def curate(curatedTriples: CuratedTriples[Interpretation]): Interpretation[CuratedTriples[Interpretation]] =
    for {
      triplesAndPersons <- extractPersons(curatedTriples.triples)
      (updatedTriples, persons) = triplesAndPersons
      projectPath      <- extractProjectPath(updatedTriples)
      maybeAccessToken <- findAccessToken(projectPath)
      projectMembers   <- findProjectMembers(projectPath)(maybeAccessToken)
      personsWithGitlabIds = merge(persons, projectMembers)
      newUpdatesGroups     = personsWithGitlabIds map prepareUpdates[Interpretation]
    } yield CuratedTriples(updatedTriples, curatedTriples.updatesGroups ++ newUpdatesGroups)
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
  } yield new PersonDetailsUpdaterImpl[IO](
    PersonExtractor[IO](),
    ProjectPathExtractor[IO](),
    accessTokenFinder,
    projectMembersFinder,
    new PersonsAndProjectMembersMatcher(),
    new UpdatesCreator
  )
}

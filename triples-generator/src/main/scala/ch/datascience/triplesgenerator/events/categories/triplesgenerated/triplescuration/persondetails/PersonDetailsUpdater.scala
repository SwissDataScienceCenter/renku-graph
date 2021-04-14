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

import cats.data.EitherT
import cats.effect.{ContextShift, Timer}
import cats.{Monad, MonadError}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.events.consumers.Project
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events.EventId
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[triplescuration] trait PersonDetailsUpdater[Interpretation[_]] {
  def updatePersonDetails(curatedTriples: CuratedTriples[Interpretation],
                          project:        Project,
                          eventId:        EventId
  ): CurationResults[Interpretation]
}

private class PersonDetailsUpdaterImpl[Interpretation[_]: Monad](
    personTrimmer:                   PersonTrimmer[Interpretation],
    accessTokenFinder:               AccessTokenFinder[Interpretation],
    projectMembersFinder:            GitLabProjectMembersFinder[Interpretation],
    personsAndProjectMembersMatcher: PersonsAndProjectMembersMatcher,
    updatesCreator:                  UpdatesCreator
)(implicit ME:                       MonadError[Interpretation, Throwable])
    extends PersonDetailsUpdater[Interpretation] {

  import IOAccessTokenFinder._
  import accessTokenFinder._
  import personsAndProjectMembersMatcher._
  import projectMembersFinder._
  import updatesCreator._

  def updatePersonDetails(curatedTriples: CuratedTriples[Interpretation],
                          project:        Project,
                          eventId:        EventId
  ): CurationResults[Interpretation] =
    for {
      maybeAccessToken <- findAccessToken(project.path).toRightT
      triplesAndPersons <-
        personTrimmer
          .getTriplesAndTrimmedPersons(curatedTriples.triples, project.id, eventId, maybeAccessToken)
      (updatedTriples, trimmedPersons) = triplesAndPersons
      projectMembers <- findProjectMembers(project.path)(maybeAccessToken)
      personsWithGitlabIds = merge(trimmedPersons, projectMembers)
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
    personTrimmer        <- IOPersonTrimmer(gitLabThrottler, logger)
  } yield new PersonDetailsUpdaterImpl[IO](
    personTrimmer,
    accessTokenFinder,
    projectMembersFinder,
    new PersonsAndProjectMembersMatcher(),
    new UpdatesCreator(gitLabUrl.apiV4)
  )
}

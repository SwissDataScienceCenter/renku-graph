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

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.{ContextShift, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.ProjectMetadata
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationData.TransformationStep
//import ch.datascience.events.consumers.Project
//import ch.datascience.graph.model.events.EventId
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationData
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[triplescuration] trait PersonDetailsUpdater[Interpretation[_]] {
  def updatePersonDetails: List[TransformationStep[Interpretation]]
}

private class PersonDetailsUpdaterImpl[Interpretation[_]: MonadThrow](
//    personTrimmer:        PersonTrimmer[Interpretation],
//    projectMembersFinder: GitLabProjectMembersFinder[Interpretation]
//    personsAndProjectMembersMatcher: PersonsAndProjectMembersMatcher,
//    updatesCreator:                  UpdatesCreator
) extends PersonDetailsUpdater[Interpretation] {

//  import personsAndProjectMembersMatcher._
//  import projectMembersFinder._
//  import updatesCreator._

  def updatePersonDetails(
      transformationData: TransformationData[Interpretation]
  ): TransformationResults[Interpretation] = ??? //for {
//    triplesAndPersons <-
//      personTrimmer.getTriplesAndTrimmedPersons(transformationData.projectMetadata, eventId)
//    (updatedTriples, trimmedPersons) = triplesAndPersons
//    _ <- findProjectMembers(project.path)(maybeAccessToken)
//    personsWithGitlabIds = merge(trimmedPersons, projectMembers)
//    newUpdatesGroups     = personsWithGitlabIds map prepareUpdates[Interpretation]
//  } yield TransformationData(updatedTriples,
//                             transformationData.projectMetadata,
//                             transformationData.steps //++ newUpdatesGroups
//  )

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
  ): IO[PersonDetailsUpdater[IO]] = ??? //for {
//    projectMembersFinder <- IOGitLabProjectMembersFinder(gitLabThrottler, logger)
//    gitLabUrl            <- GitLabUrlLoader[IO]()
//    personTrimmer <- IOPersonTrimmer(gitLabThrottler, logger)
//  } yield new PersonDetailsUpdaterImpl[IO](
//    personTrimmer,
//    projectMembersFinder
//    new PersonsAndProjectMembersMatcher(),
//    new UpdatesCreator(gitLabUrl.apiV4)
//  )
}

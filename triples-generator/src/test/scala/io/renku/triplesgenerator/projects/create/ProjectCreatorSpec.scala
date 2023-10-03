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

package io.renku.triplesgenerator.projects
package create

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.lock.Lock
import io.renku.triplesgenerator.TgDB.TsWriteLock
import io.renku.triplesgenerator.api.Generators.newProjectsGen
import io.renku.triplesgenerator.api.NewProject
import io.renku.triplesgenerator.tsprovisioning.Generators.triplesUploadFailures
import io.renku.triplesgenerator.tsprovisioning.TSProvisioner
import io.renku.triplesgenerator.tsprovisioning.triplesuploading.TriplesUploadResult
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectCreatorSpec extends AsyncFlatSpec with AsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "do nothing if the project is already created" in {

    val newProject = newProjectsGen.generateOne

    givenProjectExistenceChecking(newProject.slug, returning = true.pure[IO])

    creator.createProject(newProject).assertNoException
  }

  it should "turn the new project payload into a Project entity and pass it to the TSProvisioner " +
    "if the project does not exist yet" in {

      val newProject = newProjectsGen.generateOne

      givenProjectExistenceChecking(newProject.slug, returning = false.pure[IO])

      val project = anyProjectEntities.generateOne.to[entities.Project]
      givenPayloadConverting(newProject, returning = project)

      givenTSProvisioning(project, returning = TriplesUploadResult.DeliverySuccess.pure[IO])

      creator.createProject(newProject).assertNoException
    }

  it should "fail if TS Provisioning returns a failure" in {

    val newProject = newProjectsGen.generateOne

    givenProjectExistenceChecking(newProject.slug, returning = false.pure[IO])

    val project = anyProjectEntities.generateOne.to[entities.Project]
    givenPayloadConverting(newProject, returning = project)

    val failure = triplesUploadFailures.generateOne
    givenTSProvisioning(project, returning = failure.pure[IO])

    creator.createProject(newProject).assertThrowsError[TriplesUploadResult.TriplesUploadFailure](_ shouldBe failure)
  }

  private val tsWriteLock: TsWriteLock[IO] = Lock.none[IO, projects.Slug]
  private val projectExistenceChecker = mock[ProjectExistenceChecker[IO]]
  private val payloadConverter        = mock[PayloadConverter]
  private val tsProvisioner           = mock[TSProvisioner[IO]]
  private lazy val creator =
    new ProjectCreatorImpl[IO](projectExistenceChecker, payloadConverter, tsProvisioner, tsWriteLock)

  private def givenProjectExistenceChecking(slug: projects.Slug, returning: IO[Boolean]) =
    (projectExistenceChecker.checkExists _)
      .expects(slug)
      .returning(returning)

  private def givenPayloadConverting(newProject: NewProject, returning: entities.Project) =
    (payloadConverter.apply _)
      .expects(newProject)
      .returning(returning)

  private def givenTSProvisioning(project: entities.Project, returning: IO[TriplesUploadResult]) =
    (tsProvisioner.provisionTS _)
      .expects(project)
      .returning(returning)
}

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

package io.renku.triplesgenerator.events.categories.triplesgenerated

import cats.data.EitherT
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.events.consumers
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators.ActivityGenFactory
import io.renku.http.client.AccessToken
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.events.categories.{ProcessingNonRecoverableError, ProcessingRecoverableError}
import io.renku.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators._
import io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo.ProjectInfoFinder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

class JsonLDDeserializerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "deserializeToModel" should {

    "successfully deserialize JsonLD to the model - case of a Renku Project" in new TestCase {
      val project = anyRenkuProjectEntities
        .withDatasets(datasetEntities(provenanceNonModified))
        .withActivities(activityEntities)
        .generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val Success(results) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = consumers.Project(projectIds.generateOne, project.path),
            payload = JsonLD
              .arr(project.asJsonLD :: project.datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*)
              .flatten
              .fold(throw _, identity)
          )
        )
        .value

      results shouldBe project.to[entities.Project].asRight
    }

    "successfully deserialize JsonLD to the model - case of a Non-Renku Project" in new TestCase {
      val project = anyNonRenkuProjectEntities.generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val Success(results) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = consumers.Project(projectIds.generateOne, project.path),
            payload = project.asJsonLD.flatten.fold(throw _, identity)
          )
        )
        .value

      results shouldBe project.to[entities.Project].asRight
    }

    "fail if there's no project info found for the project" in new TestCase {
      val projectPath = projectPaths.generateOne

      givenFindProjectInfo(projectPath)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](Option.empty[GitLabProjectInfo]))

      val eventProject = consumers.Project(projectIds.generateOne, projectPath)

      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            payload = JsonLD.arr()
          )
        )
        .value

      error            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      error.getMessage shouldBe show"$eventProject not found in GitLab"
    }

    "fail if fetching the project info fails" in new TestCase {
      val projectPath = projectPaths.generateOne

      val exception = exceptions.generateOne
      givenFindProjectInfo(projectPath)
        .returning(EitherT(exception.raiseError[Try, Either[ProcessingRecoverableError, Option[GitLabProjectInfo]]]))

      deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = consumers.Project(projectIds.generateOne, projectPath),
            payload = JsonLD.arr()
          )
        )
        .value shouldBe Failure(exception)
    }

    "fail if no project is found in the JsonLD" in new TestCase {
      val project = anyRenkuProjectEntities.withDatasets(datasetEntities(provenanceNonModified)).generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)

      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            payload = JsonLD.arr(project.datasets.map(_.asJsonLD): _*).flatten.fold(throw _, identity)
          )
        )
        .value

      error            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      error.getMessage shouldBe show"0 Project entities found in the JsonLD for $eventProject"
    }

    "fail if there are other projects in the JsonLD" in new TestCase {
      val project      = anyProjectEntities.generateOne
      val otherProject = anyProjectEntities.generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)

      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            payload = JsonLD
              .arr(project.asJsonLD, otherProject.asJsonLD)
              .flatten
              .fold(throw _, identity)
          )
        )
        .value

      error            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      error.getMessage shouldBe show"2 Project entities found in the JsonLD for $eventProject"
    }

    "fail if the project found in the payload is different than the project in the event" in new TestCase {
      val project      = anyProjectEntities.generateOne
      val eventProject = consumerProjects.generateOne

      givenFindProjectInfo(eventProject.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val Failure(error) = deserializer.deserializeToModel {
        triplesGeneratedEvents.generateOne.copy(
          project = eventProject,
          payload = JsonLD.arr(project.asJsonLD).flatten.fold(throw _, identity)
        )
      }.value

      error            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      error.getMessage shouldBe show"Event for project $eventProject contains payload for project ${project.path}"
    }

    "successfully deserialize JsonLD to the model " +
      "if project from the payload has the same path in case insensitive way as the project in the event" in new TestCase {
        val project      = anyProjectEntities.generateOne
        val eventProject = consumers.Project(projectIds.generateOne, projects.Path(project.path.value.toUpperCase()))

        givenFindProjectInfo(eventProject.path)
          .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

        val Success(results) = deserializer.deserializeToModel {
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            payload = JsonLD.arr(project.asJsonLD).flatten.fold(throw _, identity)
          )
        }.value

        results shouldBe project.to[entities.Project].asRight
      }

    "fail if the payload is invalid" in new TestCase {
      val project = renkuProjectEntities(anyVisibility).generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)
      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            payload = project
              .to[entities.RenkuProject.WithoutParent]
              .copy(activities =
                activityEntities
                  .withDateBefore(project.dateCreated)
                  .generateFixedSizeList(1)
                  .map(_.to[entities.Activity])
              )
              .asJsonLD
              .flatten
              .fold(throw _, identity)
          )
        )
        .value

      error            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      error.getMessage shouldBe show"Finding Project entity in the JsonLD for $eventProject failed"
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    def gitLabProjectInfo(project: Project) = GitLabProjectInfo(
      projectIds.generateOne,
      project.name,
      project.path,
      project.dateCreated,
      project.maybeDescription,
      project.maybeCreator.map(_.to[ProjectMember]),
      project.keywords,
      project.members.map(_.to[ProjectMember]),
      project.visibility,
      maybeParentPath = project match {
        case p: Project with Parent => p.parent.path.some
        case _ => None
      }
    )

    val projectInfoFinder = mock[ProjectInfoFinder[Try]]
    val deserializer      = new JsonLDDeserializerImpl[Try](projectInfoFinder, renkuUrl)

    private implicit lazy val toProjectMember: Person => ProjectMember = person => {
      val member = ProjectMember(person.name,
                                 persons.Username(person.name.value),
                                 person.maybeGitLabId.getOrElse(fail("Project person without GitLabId"))
      )
      person.maybeEmail match {
        case Some(email) => member.add(email)
        case None        => member
      }
    }

    def givenFindProjectInfo(projectPath: projects.Path) = new {
      def returning(result: EitherT[Try, ProcessingRecoverableError, Option[GitLabProjectInfo]]) =
        (projectInfoFinder
          .findProjectInfo(_: projects.Path)(_: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(result)
    }
  }

  private def activityEntities: ActivityGenFactory = project => {
    val paramValue = parameterDefaultValues.generateOne
    val input      = entityLocations.generateOne
    val output     = entityLocations.generateOne
    executionPlanners(
      planEntities(
        CommandParameter.from(paramValue),
        CommandInput.fromLocation(input),
        CommandOutput.fromLocation(output)
      ),
      project
    ).generateOne
      .planParameterValues(paramValue -> parameterValueOverrides.generateOne)
      .planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne)
      .buildProvenanceUnsafe()
  }
}

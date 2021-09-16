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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.data.EitherT
import cats.syntax.all._
import ch.datascience.events.consumers
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model._
import ch.datascience.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import ch.datascience.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import ch.datascience.graph.model.testentities._
import ch.datascience.graph.model.testentities.generators.EntitiesGenerators.ActivityGenFactory
import ch.datascience.http.client.AccessToken
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, JsonLD, Property}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

class JsonLDDeserializerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "deserializeToModel" should {

    "successfully deserialize JsonLD to the model" in new TestCase {
      val project = anyProjectEntities
        .withDatasets(datasetEntities(ofAnyProvenance))
        .withActivities(activityEntities)
        .generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val Success(results) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = consumers.Project(projectIds.generateOne, project.path),
            triples = JsonLD
              .arr(project.asJsonLD :: project.datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*)
              .flatten
              .fold(throw _, identity)
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
            triples = JsonLD.arr()
          )
        )
        .value

      error            shouldBe a[IllegalStateException]
      error.getMessage shouldBe s"No project ${eventProject.show} found in GitLab"
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
            triples = JsonLD.arr()
          )
        )
        .value shouldBe Failure(exception)
    }

    "fail if no project is found in the JsonLD" in new TestCase {
      val project = anyProjectEntities
        .withDatasets(datasetEntities(ofAnyProvenance))
        .withActivities(activityEntities)
        .generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val triples = JsonLD
        .arr(
          project.activities.map(_.asJsonLD) :::
            project.activities.map(_.plan.asJsonLD) :::
            project.datasets.map(_.asJsonLD): _*
        )
        .flatten
        .fold(throw _, identity)

      val eventProject = consumers.Project(projectIds.generateOne, project.path)

      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            triples = triples
          )
        )
        .value

      error            shouldBe a[IllegalStateException]
      error.getMessage shouldBe s"0 Project entities found in the JsonLD for ${eventProject.show}"
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
            triples = JsonLD
              .arr(project.asJsonLD, otherProject.asJsonLD)
              .flatten
              .fold(throw _, identity)
          )
        )
        .value

      error            shouldBe a[IllegalStateException]
      error.getMessage shouldBe show"2 Project entities found in the JsonLD for $eventProject"
    }

    "fail if the payload is invalid" in new TestCase {
      val project = anyProjectEntities.generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)
      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            triples = JsonLD
              .arr(project.asJsonLD,
                   JsonLD.entity(EntityId.of(httpUrls().generateOne),
                                 entities.Activity.entityTypes,
                                 Map.empty[Property, JsonLD]
                   )
              )
              .flatten
              .fold(throw _, identity)
          )
        )
        .value

      error            shouldBe a[IllegalStateException]
      error.getMessage shouldBe s"Finding Project entity in the JsonLD for ${eventProject.show} failed"
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    def gitLabProjectInfo(project: Project) = GitLabProjectInfo(
      project.name,
      project.path,
      project.dateCreated,
      project.maybeCreator.map(_.to[ProjectMember]),
      project.members.map(_.to[ProjectMember]),
      project.visibility,
      maybeParentPath = project match {
        case p: ProjectWithParent    => p.parent.path.some
        case _: ProjectWithoutParent => None
      }
    )

    val projectInfoFinder = mock[ProjectInfoFinder[Try]]
    val deserializer      = new JsonLDDeserializerImpl[Try](projectInfoFinder, renkuBaseUrl)

    private implicit lazy val toProjectMember: Person => ProjectMember = person =>
      ProjectMember(person.name,
                    users.Username(person.name.value),
                    person.maybeGitLabId.getOrElse(fail("Project person without GitLabId"))
      )

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

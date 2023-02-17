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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning
package triplesgenerated

import CategoryGenerators._
import cats.data.EitherT.rightT
import cats.data.{EitherT, Kleisli}
import cats.syntax.all._
import cats.{Foldable, Functor}
import io.renku.cli.model.CliProject
import io.renku.events.consumers
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model._
import io.renku.graph.model.cli.CliEntityConverterSyntax
import io.renku.graph.model.entities.DiffInstances
import io.renku.graph.model.entities.Project.ProjectMember.{ProjectMemberNoEmail, ProjectMemberWithEmail}
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.testentities.ModelOps.toEntitiesInternal
import io.renku.graph.model.testentities.StepPlanCommandParameter.{CommandInput, CommandOutput, CommandParameter}
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.testentities.generators.EntitiesGenerators.ActivityGenFactory
import io.renku.graph.model.testentities.{Parent, Person, Project}
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.cli.model.tools.JsonLDTools.{flattenedJsonLD, flattenedJsonLDFrom}
import io.renku.http.client.AccessToken
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{EitherValues, TryValues}
import projectinfo.ProjectInfoFinder

import scala.language.reflectiveCalls
import scala.util.Try

class EntityBuilderSpec
    extends AnyWordSpec
    with MockFactory
    with EntitiesGenerators
    with should.Matchers
    with AdditionalMatchers
    with CliEntityConverterSyntax
    with TryValues
    with EitherValues
    with DiffInstances {

  "buildEntity" should {

    "successfully deserialize JsonLD to the model - case of a Renku Project" in new TestCase {

      val testProject = anyRenkuProjectEntities(anyVisibility, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .withDatasets(datasetEntities(provenanceNonModified(creatorsGen = cliShapedPersons)))
        .withActivities(activityEntities)
        .generateOne

      val glProject = gitLabProjectInfo(testProject)
      givenFindProjectInfo(testProject.path)
        .returning(rightT[Try, ProcessingRecoverableError](glProject.some))

      val modelProject = testProject.to[entities.Project]
      val cliProject   = testProject.to[CliProject]

      val results = entityBuilder
        .buildEntity(
          triplesGeneratedEvents.generateOne.copy(
            project = consumers.Project(projectIds.generateOne, testProject.path),
            payload = cliProject.asJsonLD(CliProject.flatJsonLDEncoder)
          )
        )
        .value

      results.success.value shouldMatchToRight combine(modelProject, glProject)
    }

    "successfully deserialize JsonLD to the model - case of a Non-Renku Project" in new TestCase {

      val testProject = anyNonRenkuProjectEntities(creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .generateOne

      val glProject = gitLabProjectInfo(testProject)
      givenFindProjectInfo(testProject.path)
        .returning(rightT[Try, ProcessingRecoverableError](glProject.some))

      val modelProject = testProject.to[entities.Project]
      val cliProject   = modelProject.toCliEntity

      val results = entityBuilder
        .buildEntity(
          triplesGeneratedEvents.generateOne.copy(
            project = consumers.Project(projectIds.generateOne, testProject.path),
            payload = payloadJsonLD(cliProject)
          )
        )
        .value

      results.success.value shouldMatchToRight combine(modelProject, glProject)
    }

    "fail if there's no project info found for the project" in new TestCase {

      val projectPath = projectPaths.generateOne

      givenFindProjectInfo(projectPath)
        .returning(rightT[Try, ProcessingRecoverableError](Option.empty[GitLabProjectInfo]))

      val eventProject = consumers.Project(projectIds.generateOne, projectPath)
      val results = entityBuilder
        .buildEntity(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            payload = JsonLD.arr()
          )
        )
        .value

      results.failure.exception            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      results.failure.exception.getMessage shouldBe show"$eventProject not found in GitLab"
    }

    "fail if fetching the project info fails" in new TestCase {

      val projectPath = projectPaths.generateOne

      val exception = exceptions.generateOne
      givenFindProjectInfo(projectPath)
        .returning(EitherT(exception.raiseError[Try, Either[ProcessingRecoverableError, Option[GitLabProjectInfo]]]))

      entityBuilder
        .buildEntity(
          triplesGeneratedEvents.generateOne.copy(
            project = consumers.Project(projectIds.generateOne, projectPath),
            payload = JsonLD.arr()
          )
        )
        .value shouldBe exception.raiseError[Try, Either[ProcessingRecoverableError, entities.Project]]
    }

    "fail if no project is found in the JsonLD" in new TestCase {

      val testProject = anyRenkuProjectEntities(anyVisibility, creatorGen = cliShapedPersons)
        .modify(removeMembers())
        .withDatasets(datasetEntities(provenanceNonModified(creatorsGen = cliShapedPersons)))
        .generateOne

      val eventProject = consumers.Project(projectIds.generateOne, testProject.path)

      givenFindProjectInfo(testProject.path)
        .returning(rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(testProject).some))

      val results = entityBuilder
        .buildEntity(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            payload = flattenedJsonLD(agentEntities.generateOne)
          )
        )
        .value

      results.failure.exception            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      results.failure.exception.getMessage shouldBe show"0 Project entities found in the JsonLD for $eventProject"
    }

    "fail if there are other projects in the JsonLD" in new TestCase {

      val project      = projectEntities(anyVisibility, creatorGen = cliShapedPersons).map(removeMembers()).generateOne
      val otherProject = projectEntities(anyVisibility, creatorGen = cliShapedPersons).map(removeMembers()).generateOne

      givenFindProjectInfo(project.path)
        .returning(rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)

      val results = entityBuilder
        .buildEntity(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            payload = flattenedJsonLDFrom(project.to[entities.Project].toCliEntity.asJsonLD,
                                          otherProject.to[entities.Project].toCliEntity.asJsonLD
            )
          )
        )
        .value

      results.failure.exception            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      results.failure.exception.getMessage shouldBe show"2 Project entities found in the JsonLD for $eventProject"
    }

    "fail if the project found in the payload is different than the project in the event" in new TestCase {

      val project      = projectEntities(anyVisibility, creatorGen = cliShapedPersons).map(removeMembers()).generateOne
      val eventProject = consumerProjects.generateOne

      givenFindProjectInfo(eventProject.path)
        .returning(rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val results = entityBuilder.buildEntity {
        triplesGeneratedEvents.generateOne.copy(
          project = eventProject,
          payload = payloadJsonLD(project.to[entities.Project].toCliEntity)
        )
      }.value

      results.failure.exception shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      results.failure.exception.getMessage shouldBe show"Event for project $eventProject contains payload for project ${project.path}"
    }

    "successfully deserialize JsonLD to the model " +
      "if project from the payload has the same path in case insensitive way as the project in the event" in new TestCase {

        val project = projectEntities(anyVisibility, creatorGen = cliShapedPersons).map(removeMembers()).generateOne
        val eventProject = consumers.Project(projectIds.generateOne, projects.Path(project.path.value.toUpperCase()))

        val glProject = gitLabProjectInfo(project)
        givenFindProjectInfo(eventProject.path)
          .returning(rightT[Try, ProcessingRecoverableError](glProject.some))

        val modelProject = project.to[entities.Project]
        val results = entityBuilder.buildEntity {
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            payload = payloadJsonLD(modelProject.toCliEntity)
          )
        }.value

        results.success.value shouldMatchToRight combine(modelProject, glProject)
      }

    "fail if the payload is invalid" in new TestCase {

      val project = renkuProjectEntities(anyVisibility, creatorGen = cliShapedPersons).map(removeMembers()).generateOne

      givenFindProjectInfo(project.path)
        .returning(rightT[Try, ProcessingRecoverableError](gitLabProjectInfo(project).some))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)

      val brokenDs = datasetEntities(provenanceInternal(cliShapedPersons))
        .withDateBefore(projects.DateCreated(project.dateCreated.value.minusSeconds(1)))
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Internal]]
      val brokenModelProject = project
        .to[entities.RenkuProject.WithoutParent]
        .copy(datasets = List(brokenDs))

      val results = entityBuilder
        .buildEntity(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            payload = payloadJsonLD(brokenModelProject.toCliEntity)
          )
        )
        .value

      results.failure.exception            shouldBe a[ProcessingNonRecoverableError.MalformedRepository]
      results.failure.exception.getMessage shouldBe show"Finding Project entity in the JsonLD for $eventProject failed"
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
      projectMembers.generateSet(),
      project.visibility,
      maybeParentPath = project match {
        case p: Project with Parent => p.parent.path.some
        case _ => None
      },
      project.images.headOption
    )

    private val projectInfoFinder = mock[ProjectInfoFinder[Try]]
    val entityBuilder             = new EntityBuilderImpl[Try](projectInfoFinder, renkuUrl)

    private implicit lazy val toProjectMember: Person => ProjectMember = person => {
      val member = ProjectMember(person.name, persons.Username(person.name.value), personGitLabIds.generateOne)
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

  private def activityEntities: ActivityGenFactory = Kleisli { projectDateCreated =>
    val paramValue = parameterDefaultValues.generateOne
    val input      = entityLocations.generateOne
    val output     = entityLocations.generateOne
    executionPlanners(
      stepPlanEntities(
        planCommands,
        cliShapedPersons,
        CommandParameter.from(paramValue),
        CommandInput.fromLocation(input),
        CommandOutput.fromLocation(output)
      ),
      projectDateCreated,
      authorGen = cliShapedPersons
    ).generateOne
      .planParameterValues(paramValue -> parameterValueOverrides.generateOne)
      .planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne)
      .buildProvenanceUnsafe()
  }

  private def payloadJsonLD(project: CliProject) =
    flattenedJsonLDFrom(project.asJsonLD, project.datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*)

  private def combine(modelProject: entities.Project, glProject: GitLabProjectInfo) = {
    val creatorWithGLId = blend(modelProject.maybeCreator, glProject.maybeCreator)
    modelProject.fold(
      _.copy(maybeCreator = creatorWithGLId, members = glProject.members.map(toPerson)),
      _.copy(maybeCreator = creatorWithGLId, members = glProject.members.map(toPerson)),
      _.copy(maybeCreator = creatorWithGLId, members = glProject.members.map(toPerson)),
      _.copy(maybeCreator = creatorWithGLId, members = glProject.members.map(toPerson))
    )
  }

  private def blend[F[_]: Functor: Foldable](persons: F[entities.Person],
                                             members: F[ProjectMember]
  ): F[entities.Person] =
    persons.map(p =>
      members
        .find(m => m.name == p.name || m.username.value == p.name.value)
        .map(m => p.add(m.gitLabId))
        .getOrElse(p)
    )

  private def toPerson(projectMember: ProjectMember): entities.Person = projectMember match {
    case ProjectMemberNoEmail(name, _, gitLabId) =>
      entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                   gitLabId,
                                   name,
                                   maybeEmail = None,
                                   maybeOrcidId = None,
                                   maybeAffiliation = None
      )
    case ProjectMemberWithEmail(name, _, gitLabId, email) =>
      entities.Person.WithGitLabId(persons.ResourceId(gitLabId),
                                   gitLabId,
                                   name,
                                   email.some,
                                   maybeOrcidId = None,
                                   maybeAffiliation = None
      )
  }
}

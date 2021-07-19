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
import ch.datascience.graph.model.datasets.TopmostSameAs
import ch.datascience.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import ch.datascience.graph.model.testentities._
import ch.datascience.http.client.AccessToken
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators._
import io.circe.DecodingFailure
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, Property}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

class JsonLDDeserializerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "deserializeToModel" should {

    "successfully deserialize JsonLD to the model" in new TestCase {
      val activity = generateActivity(project)
      val dataset = Gen
        .oneOf(
          datasetEntities(datasetProvenanceInternal, fixed(project)),
          datasetEntities(datasetProvenanceImportedExternal, fixed(project)),
          datasetEntities(datasetProvenanceImportedInternalAncestorInternal, fixed(project))
            .map(ds => ds.copy(provenance = ds.provenance.copy(topmostSameAs = TopmostSameAs(ds.provenance.sameAs)))),
          datasetEntities(datasetProvenanceImportedInternalAncestorExternal, fixed(project))
            .map(ds => ds.copy(provenance = ds.provenance.copy(topmostSameAs = TopmostSameAs(ds.provenance.sameAs)))),
          datasetEntities(datasetProvenanceModified, fixed(project))
        )
        .generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo.some))

      val Success(results) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = consumers.Project(projectIds.generateOne, project.path),
            triples = JsonLD.arr(project.asJsonLD, activity.asJsonLD, dataset.asJsonLD).flatten.fold(throw _, identity)
          )
        )
        .value

      results shouldBe a[Right[_, _]]

      val Right(metadata) = results
      metadata.project  shouldBe project.to[entities.Project]
      metadata.activities should contain theSameElementsAs List(activity.to[entities.Activity])
      val actual   = metadata.datasets
      val expected = List(dataset)

      actual should contain theSameElementsAs expected
    }

    "fail if there's no project info found for the project" in new TestCase {
      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](Option.empty[GitLabProjectInfo]))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)

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
      val exception = exceptions.generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT(exception.raiseError[Try, Either[ProcessingRecoverableError, Option[GitLabProjectInfo]]]))

      deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = consumers.Project(projectIds.generateOne, project.path),
            triples = JsonLD.arr()
          )
        )
        .value shouldBe Failure(exception)
    }

    "fail if no project is found in the JsonLD" in new TestCase {
      val activity = generateActivity(project)
      val dataset  = datasetEntities(ofAnyProvenance, fixed(project)).generateOne

      lazy val filterOutProject: Vector[JsonLD] => JsonLD = { array =>
        JsonLD.arr(array.filter(_.cursor.getEntityTypes.map(_ != Project.types).getOrElse(false)): _*)
      }

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo.some))

      val triples = JsonLD
        .arr(activity.asJsonLD, dataset.asJsonLD)
        .flatten
        .fold(throw _, _.asArray.fold(fail("JsonLD is not an array"))(filterOutProject))

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
      val activity     = generateActivity(project)
      val otherProject = projectEntities(visibilityAny)(anyForksCount).generateOne
      val dataset      = datasetEntities(ofAnyProvenance, fixed(otherProject)).generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo.some))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)
      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            triples = JsonLD
              .arr(project.asJsonLD, activity.asJsonLD, dataset.asJsonLD)
              .flatten
              .fold(throw _, identity)
          )
        )
        .value
      error            shouldBe a[IllegalStateException]
      error.getMessage shouldBe s"Finding Project entity in the JsonLD for ${eventProject.show} failed"
      error.getCause   shouldBe a[DecodingFailure]
    }

    "fail if decoding Person entities fails" in new TestCase {
      val triples = JsonLD
        .arr(project.asJsonLD,
             JsonLD.entity(userResourceIds.generateOne.asEntityId,
                           entities.Person.entityTypes,
                           Map.empty[Property, JsonLD]
             )
        )
        .flatten
        .fold(throw _, identity)

      val eventProject = consumers.Project(projectIds.generateOne, project.path)
      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(project = eventProject, triples = triples)
        )
        .value

      error            shouldBe a[IllegalStateException]
      error.getMessage shouldBe s"Finding Person entities in the JsonLD for ${eventProject.show} failed"
    }

    "fail if decoding Activity entities fails" in new TestCase {
      val activity = JsonLD.entity(activities.ResourceId(httpUrls().generateOne).asEntityId,
                                   entities.Activity.entityTypes,
                                   Map.empty[Property, JsonLD]
      )

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo.some))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)
      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            triples = JsonLD.arr(project.asJsonLD, activity).flatten.fold(throw _, identity)
          )
        )
        .value

      error            shouldBe a[IllegalStateException]
      error.getMessage shouldBe s"Finding Activity entities in the JsonLD for ${eventProject.show} failed"
    }

    "fail if decoding Dataset entities fails" in new TestCase {
      val dataset = JsonLD.entity(datasets.ResourceId(httpUrls().generateOne).asEntityId,
                                  entities.Dataset.entityTypes,
                                  Map.empty[Property, JsonLD]
      )

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo.some))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)
      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            triples = JsonLD.arr(project.asJsonLD, dataset).flatten.fold(throw _, identity)
          )
        )
        .value

      error            shouldBe a[IllegalStateException]
      error.getMessage shouldBe s"Finding Dataset entities in the JsonLD for ${eventProject.show} failed"
    }

    "fail if the payload is invalid" in new TestCase {
      val activity = {
        val a = generateActivity(project)
        a.copy(startTime = timestamps(max = project.dateCreated.value).generateAs[activities.StartTime])
      }
      val dataset = datasetEntities(ofAnyProvenance, fixed(project)).generateOne

      givenFindProjectInfo(project.path)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](gitLabProjectInfo.some))

      val eventProject = consumers.Project(projectIds.generateOne, project.path)
      val Failure(error) = deserializer
        .deserializeToModel(
          triplesGeneratedEvents.generateOne.copy(
            project = eventProject,
            triples = JsonLD.arr(project.asJsonLD, activity.asJsonLD, dataset.asJsonLD).flatten.fold(throw _, identity)
          )
        )
        .value

      error          shouldBe a[IllegalStateException]
      error.getMessage should startWith(s"Invalid payload for project ${eventProject.show}: ")
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val project = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne
    val gitLabProjectInfo = GitLabProjectInfo(
      project.name,
      project.path,
      project.dateCreated,
      project.maybeCreator.map(_.to[ProjectMember]),
      project.members.map(_.to[ProjectMember]),
      project.visibility,
      maybeParentPath = None
    )

    private val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne
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

  private def generateActivity(project: Project[ForksCount]) = {
    val paramValue = parameterDefaultValues.generateOne
    val input      = entityLocations.generateOne
    val output     = entityLocations.generateOne
    executionPlanners(
      runPlanEntities(
        CommandParameter.from(paramValue),
        CommandInput.fromLocation(input),
        CommandOutput.fromLocation(output)
      ),
      fixed(project)
    ).generateOne
      .planParameterValues(paramValue -> parameterValueOverrides.generateOne)
      .planInputParameterValuesFromChecksum(input -> entityChecksums.generateOne)
      .buildProvenanceGraph
      .fold(errors => fail(errors.toList.mkString), identity)
  }
}

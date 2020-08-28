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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError._
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators.curatedTriplesObjects
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.IOTriplesCurator.CurationRecoverableError
import eu.timepit.refined.auto._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.{higherKinds, reflectiveCalls}

class PayloadTransformerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "transform" should {

    "do nothing if both projects from GitLab and KG have no forks" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = emptyOptionOf[Path]).generateOne).existsInGitLab
      given(kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne).existsInKG

      transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Right(givenCuratedTriples)
    }

    "transform the triples if Gitlab project has fork" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = projectPaths.generateSome).generateOne).existsInGitLab
      given(kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne).existsInKG
      val transformedTriples = givenTriplesTransformationCalled()

      transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Right(transformedTriples)
    }
    "transform the triples if KG project has fork" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = emptyOptionOf[Path]).generateOne).existsInGitLab
      given(kgProjects(maybeParentResourceIds = projectResourceIds.generateSome).generateOne).existsInKG
      val transformedTriples = givenTriplesTransformationCalled()

      transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Right(transformedTriples)
    }
    "transform the triples if both KG and Gitlab project have forks" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = projectPaths.generateSome).generateOne).existsInGitLab
      given(kgProjects(maybeParentResourceIds = projectResourceIds.generateSome).generateOne).existsInKG
      val transformedTriples = givenTriplesTransformationCalled()

      transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Right(transformedTriples)
    }

    "do nothing if no projects in GitLab and in KG" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = emptyOptionOf[Path]).generateOne).doesNotExistsInGitLab
      given(kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne).doesNotExistsInKG

      transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Right(givenCuratedTriples)
    }

    "do nothing if no projects in GitLab" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = emptyOptionOf[Path]).generateOne).doesNotExistsInGitLab
      given(kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne).existsInKG

      transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Right(givenCuratedTriples)
    }

    "do nothing if no projects in KG" in new TestCase {

      given(gitLabProjects(maybeParentPaths   = emptyOptionOf[Path]).generateOne).existsInGitLab
      given(kgProjects(maybeParentResourceIds = emptyOptionOf[ResourceId]).generateOne).doesNotExistsInKG

      transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Right(givenCuratedTriples)
    }

    Set(
      BadRequestException(nonBlankStrings().generateOne),
      MappingException(nonBlankStrings().generateOne, exceptions.generateOne),
      UnauthorizedException
    ) foreach { exception =>
      s"fail if finding GitLab project fails with ${exception.getClass}" in new TestCase {

        given(kgProjects().generateOne).existsInKG

        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(exception.raiseError[IO, Option[GitLabProject]])

        intercept[Exception] {
          transformer.transform(event, givenCuratedTriples).value.unsafeRunSync()
        } shouldBe exception
      }

      s"fail if finding KG project fails with ${exception.getClass}" in new TestCase {

        given(gitLabProjects(maybeParentPaths = emptyOptionOf[Path]).generateOne).existsInGitLab

        (kgInfoFinder
          .findProject(_: Path))
          .expects(event.project.path)
          .returning(exception.raiseError[IO, Option[KGProject]])

        intercept[Exception] {
          transformer.transform(event, givenCuratedTriples).value.unsafeRunSync()
        } shouldBe exception
      }
    }

    Set(
      UnexpectedResponseException(nonBlankStrings().generateOne),
      ConnectivityException(nonBlankStrings().generateOne, exceptions.generateOne)
    ) foreach { exception =>
      s"return $CurationRecoverableError if finding GitLab project fails with ${exception.getClass.getSimpleName}" in new TestCase {

        given(kgProjects().generateOne).existsInKG

        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(exception.raiseError[IO, Option[GitLabProject]])

        transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Left {
          CurationRecoverableError("Problem with finding fork info", exception)
        }
      }

      s"return $CurationRecoverableError if finding KG project fails ${exception.getClass.getSimpleName}" in new TestCase {

        given(gitLabProjects(maybeParentPaths = emptyOptionOf[Path]).generateOne).existsInGitLab

        (kgInfoFinder
          .findProject(_: Path))
          .expects(event.project.path)
          .returning(exception.raiseError[IO, Option[KGProject]])

        transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Left {
          CurationRecoverableError("Problem with finding fork info", exception)
        }
      }
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val event               = commitEvents.generateOne
    val givenCuratedTriples = jsonLDTriples.generateOne
    val triplesTransformer  = mockFunction[JsonLDTriples, JsonLDTriples]

    val gitLabInfoFinder = mock[GitLabInfoFinder[IO]]
    val kgInfoFinder     = mock[KGInfoFinder[IO]]

    val transformer = new PayloadTransformerImpl(gitLabInfoFinder, kgInfoFinder, triplesTransformer)

    def givenTriplesTransformationCalled(): JsonLDTriples = {
      val transformedTriples = jsonLDTriples.generateOne
      triplesTransformer.expects(givenCuratedTriples).returning(transformedTriples)
      transformedTriples
    }

    def given(gitLabProject: GitLabProject) = new {
      lazy val existsInGitLab: GitLabProject = {
        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(Option(gitLabProject).pure[IO])
        gitLabProject
      }

      lazy val doesNotExistsInGitLab = {
        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(Option.empty.pure[IO])
      }
    }

    def given(kgProject: KGProject) = new {
      lazy val existsInKG: KGProject = {
        (kgInfoFinder
          .findProject(_: Path))
          .expects(event.project.path)
          .returning(Option(kgProject).pure[IO])
        kgProject
      }

      lazy val doesNotExistsInKG = {
        (kgInfoFinder
          .findProject(_: Path))
          .expects(event.project.path)
          .returning(Option.empty.pure[IO])
      }
    }
  }
}

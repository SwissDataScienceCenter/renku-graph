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

package ch.datascience.triplesgenerator.events.awaitinggeneration.triplescuration.forks

import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.Path
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError._
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.awaitinggeneration.EventProcessingGenerators._
import ch.datascience.triplesgenerator.events.awaitinggeneration.triplescuration.IOTriplesCurator.CurationRecoverableError
import eu.timepit.refined.auto._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

class PayloadTransformerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "transform" should {

    "do nothing if there's no project in GitLab" in new TestCase {

      given(
        gitLabProjects(event.project.path, maybeParentPaths = emptyOptionOf[Path]).generateOne
      ).doesNotExistsInGitLab

      transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Right(givenCuratedTriples)
    }

    "transform the triples if there's project in Gitlab" in new TestCase {

      given(gitLabProjects(event.project.path, maybeParentPaths = projectPaths.generateSome).generateOne).existsInGitLab
      val transformedTriples = givenTriplesTransformationCalled()

      transformer.transform(event, givenCuratedTriples).value.unsafeRunSync() shouldBe Right(transformedTriples)
    }

    Set(
      BadRequestException(nonBlankStrings().generateOne),
      MappingException(nonBlankStrings().generateOne, exceptions.generateOne),
      UnauthorizedException
    ) foreach { exception =>
      s"fail if finding GitLab project fails with ${exception.getClass}" in new TestCase {

        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(exception.raiseError[IO, Option[GitLabProject]])

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

        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(exception.raiseError[IO, Option[GitLabProject]])

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

    val transformer = new PayloadTransformerImpl(gitLabInfoFinder, triplesTransformer)

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

      lazy val doesNotExistsInGitLab =
        (gitLabInfoFinder
          .findProject(_: Path)(_: Option[AccessToken]))
          .expects(event.project.path, maybeAccessToken)
          .returning(Option.empty.pure[IO])
    }
  }
}

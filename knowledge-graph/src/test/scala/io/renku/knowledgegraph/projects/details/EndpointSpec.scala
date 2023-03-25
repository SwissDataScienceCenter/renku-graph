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

package io.renku.knowledgegraph.projects.details

import ProjectsGenerators._
import cats.effect.IO
import cats.syntax.all._
import io.circe.Json
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.graph.model.{projects, GitLabUrl}
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.knowledgegraph.projects.details.model.Project
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import org.http4s.{Headers, Request}
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s.headers.{`Content-Type`, Accept}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class EndpointSpec
    extends AnyWordSpec
    with MockFactory
    with TableDrivenPropertyChecks
    with should.Matchers
    with IOSpec {

  "GET /projects/:path" should {

    forAll {
      Table(
        "case"                     -> "request",
        "no Accept"                -> Request[IO](),
        "Accept: application/json" -> Request[IO](headers = Headers(Accept(application.json)))
      )
    } { case (caze, request) =>
      "sends a PROJECT_VIEWED event and " +
        "respond with OK with application/json and the found project details " +
        s"when there's $caze header in the request" in new TestCase {

          givenProjectFinding(project.path, returning = project.some.pure[IO])

          val json = jsons.generateOne
          (projectJsonEncoder.encode(_: Project)(_: GitLabUrl)).expects(project, gitLabUrl).returns(json)

          givenProjectViewedEventSent(project.path, returning = ().pure[IO])

          val response = endpoint.`GET /projects/:path`(project.path, maybeAuthUser)(request).unsafeRunSync()

          response.status                   shouldBe Ok
          response.contentType              shouldBe Some(`Content-Type`(application.json))
          response.as[Json].unsafeRunSync() shouldBe json

          logger.loggedOnly(
            Warn(s"Finding '${project.path}' details finished${executionTimeRecorder.executionTimeInfo}")
          )
          logger.reset()
        }
    }

    "sends a PROJECT_VIEWED event and " +
      "respond with OK with application/ld+json and the found project details " +
      "when there's Accept: application/ld+json header in the request" in new TestCase {

        givenProjectFinding(project.path, returning = project.some.pure[IO])

        val jsonLD = jsonLDEntities.generateOne
        (projectJsonLDEncoder.encode _).expects(project).returns(jsonLD)

        givenProjectViewedEventSent(project.path, returning = ().pure[IO])

        val request  = Request[IO](headers = Headers(Accept(application.`ld+json`)))
        val response = endpoint.`GET /projects/:path`(project.path, maybeAuthUser)(request).unsafeRunSync()

        response.status                   shouldBe Ok
        response.contentType              shouldBe Some(`Content-Type`(application.`ld+json`))
        response.as[Json].unsafeRunSync() shouldBe jsonLD.toJson

        logger.loggedOnly(
          Warn(s"Finding '${project.path}' details finished${executionTimeRecorder.executionTimeInfo}")
        )
      }

    forAll {
      Table(
        ("case", "request", "content-type"),
        ("no Accept", Request[IO](), application.json),
        ("Accept: application/json", Request[IO](headers = Headers(Accept(application.json))), application.json),
        ("Accept: application/ld+json",
         Request[IO](headers = Headers(Accept(application.`ld+json`))),
         application.`ld+json`
        )
      )
    } { case (caze, request, contentType) =>
      s"respond with NOT_FOUND if there is no project with the given path - $caze header" in new TestCase {

        givenProjectFinding(project.path, returning = None.pure[IO])

        val response = endpoint.`GET /projects/:path`(project.path, maybeAuthUser)(request).unsafeRunSync()

        response.status                          shouldBe NotFound
        response.contentType                     shouldBe Some(`Content-Type`(contentType))
        response.as[Json].unsafeRunSync().noSpaces should include(s"No '${project.path}' project found")
      }

      s"respond with INTERNAL_SERVER_ERROR if finding project details fails - $caze header" in new TestCase {

        val exception = exceptions.generateOne
        givenProjectFinding(project.path, returning = exception.raiseError[IO, Option[Project]])

        val response = endpoint.`GET /projects/:path`(project.path, maybeAuthUser)(request).unsafeRunSync()

        response.status      shouldBe InternalServerError
        response.contentType shouldBe Some(`Content-Type`(contentType))

        response.as[Json].unsafeRunSync().noSpaces should include(s"Finding '${project.path}' project failed")

        logger.loggedOnly(Error(s"Finding '${project.path}' project failed", exception))
      }
    }

    "do not fail if sending PROJECT_VIEWED event fails" in new TestCase {

      givenProjectFinding(project.path, returning = project.some.pure[IO])

      (projectJsonEncoder.encode(_: Project)(_: GitLabUrl)).expects(project, gitLabUrl).returns(jsons.generateOne)

      val exception = exceptions.generateOne
      givenProjectViewedEventSent(project.path, returning = exception.raiseError[IO, Unit])

      endpoint.`GET /projects/:path`(project.path, maybeAuthUser)(Request[IO]()).unsafeRunSync().status shouldBe Ok

      logger.logged(Error(show"sending ${ProjectViewedEvent.categoryName} event failed", exception))
    }
  }

  private implicit lazy val gitLabUrl: GitLabUrl = EntitiesGenerators.gitLabUrl

  private trait TestCase {
    val project       = resourceProjects.generateOne
    val maybeAuthUser = authUsers.generateOption

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val projectFinder = mock[ProjectFinder[IO]]
    val projectJsonEncoder    = mock[ProjectJsonEncoder]
    val projectJsonLDEncoder  = mock[ProjectJsonLDEncoder]
    val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
    private val tgClient      = mock[triplesgenerator.api.events.Client[IO]]
    private val currentTime   = Instant.now()
    private val now           = mockFunction[Instant]
    now.expects().returning(currentTime).anyNumberOfTimes()
    val endpoint = new EndpointImpl[IO](
      projectFinder,
      projectJsonEncoder,
      projectJsonLDEncoder,
      tgClient,
      executionTimeRecorder,
      EntitiesGenerators.gitLabUrl,
      now
    )

    def givenProjectFinding(path: projects.Path, returning: IO[Option[Project]]) =
      (projectFinder.findProject _)
        .expects(path, maybeAuthUser)
        .returning(returning)

    def givenProjectViewedEventSent(path: projects.Path, returning: IO[Unit]) =
      (tgClient
        .send(_: ProjectViewedEvent))
        .expects(ProjectViewedEvent.forProject(path, maybeAuthUser.map(_.id), now))
        .returning(returning)
  }
}

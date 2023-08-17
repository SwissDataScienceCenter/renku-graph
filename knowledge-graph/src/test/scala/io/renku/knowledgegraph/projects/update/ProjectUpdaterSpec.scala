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

package io.renku.knowledgegraph.projects.update

import Generators._
import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax._
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, jsons}
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesgenerator.api.{TriplesGeneratorClient, ProjectUpdates => TGProjectUpdates}
import org.http4s.Status.{Accepted, BadRequest, InternalServerError, Conflict}
import org.http4s.circe._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectUpdaterSpec extends AsyncFlatSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "if only GL update needed " +
    "send update only to GL and TG " +
    "and return 202 Accepted when no failures" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates = projectUpdatesGen
        .map(_.copy(newDescription = None, newKeywords = None))
        .suchThat(_.onlyGLUpdateNeeded)
        .generateOne

      givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = EitherT.pure[IO, Json](()))
      givenSendingUpdateToTG(slug, updates, returning = TriplesGeneratorClient.Result.success(()).pure[IO])

      updater.updateProject(slug, updates, authUser) >>= { response =>
        response.pure[IO].asserting(_.status shouldBe Accepted) >>
          response.as[Json].asserting(_ shouldBe Message.Info("Project update accepted").asJson)
      }
    }

  it should "if core update needed " +
    "check if pushing to the default branch is allowed " +
    "and return 409 Conflict if it's not" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenBranchProtectionChecking(slug, authUser.accessToken, returning = false.pure[IO])

      updater.updateProject(slug, updates, authUser) >>= { response =>
        response.pure[IO].asserting(_.status shouldBe Conflict) >>
          response
            .as[Json]
            .asserting(
              _ shouldBe Message
                .Error("Updating project not possible; quite likely the user cannot push to the default branch")
                .asJson
            )
      }
    }

  it should "return 400 BadRequest if GL returns 400" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates = projectUpdatesGen
      .map(_.copy(newDescription = None, newKeywords = None))
      .suchThat(_.onlyGLUpdateNeeded)
      .generateOne

    val error = jsons.generateOne
    givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = EitherT.left(error.pure[IO]))

    updater.updateProject(slug, updates, authUser) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe BadRequest) >>
        response.as[Message].asserting(_ shouldBe Message.Error.fromJsonUnsafe(error))
    }
  }

  it should "fail if updating GL fails" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates = projectUpdatesGen
      .map(_.copy(newDescription = None, newKeywords = None))
      .suchThat(_.onlyGLUpdateNeeded)
      .generateOne

    val exception = exceptions.generateOne
    givenUpdatingProjectInGL(slug,
                             updates,
                             authUser.accessToken,
                             returning = EitherT(exception.raiseError[IO, Either[Json, Unit]])
    )

    updater.updateProject(slug, updates, authUser).assertThrowsError[Exception](_ shouldBe exception)
  }

  it should "return 500 InternalServerError if updating project in TG failed" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates = projectUpdatesGen
      .map(_.copy(newDescription = None, newKeywords = None))
      .suchThat(_.onlyGLUpdateNeeded)
      .generateOne

    givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = EitherT.pure[IO, Json](()))
    val exception = exceptions.generateOne
    givenSendingUpdateToTG(slug,
                           updates,
                           returning = TriplesGeneratorClient.Result.failure(exception.getMessage).pure[IO]
    )

    updater.updateProject(slug, updates, authUser) >>= { response =>
      response.pure[IO].asserting(_.status shouldBe InternalServerError) >>
        response.as[Message].asserting(_ shouldBe Message.Error("Update failed"))
    }
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
  private val branchProtectionCheck = mock[BranchProtectionCheck[IO]]
  private val glProjectUpdater      = mock[GLProjectUpdater[IO]]
  private val tgClient              = mock[TriplesGeneratorClient[IO]]
  private lazy val updater          = new ProjectUpdaterImpl[IO](branchProtectionCheck, glProjectUpdater, tgClient)

  private def givenBranchProtectionChecking(slug: projects.Slug, at: AccessToken, returning: IO[Boolean]) =
    (branchProtectionCheck.canPushToDefaultBranch _)
      .expects(slug, at)
      .returning(returning)

  private def givenUpdatingProjectInGL(slug:      projects.Slug,
                                       updates:   ProjectUpdates,
                                       at:        AccessToken,
                                       returning: EitherT[IO, Json, Unit]
  ) = (glProjectUpdater.updateProject _)
    .expects(slug, updates, at)
    .returning(returning)

  private def givenSendingUpdateToTG(slug:      projects.Slug,
                                     updates:   ProjectUpdates,
                                     returning: IO[TriplesGeneratorClient.Result[Unit]]
  ) = (tgClient.updateProject _)
    .expects(
      slug,
      TGProjectUpdates(newDescription = updates.newDescription,
                       newImages = updates.newImage.map(_.toList),
                       newKeywords = updates.newKeywords,
                       newVisibility = updates.newVisibility
      )
    )
    .returning(returning)
}

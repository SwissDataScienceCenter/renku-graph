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
import cats.effect.IO
import cats.syntax.all._
import io.renku.core.client.Generators.{coreUrisVersioned, resultDetailedFailures, resultSuccesses, userInfos}
import io.renku.core.client.{RenkuCoreClient, RenkuCoreUri, Result, UserInfo, ProjectUpdates => CoreProjectUpdates}
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, jsons}
import io.renku.graph.model.RenkuTinyTypeGenerators.{projectGitHttpUrls, projectSlugs}
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, UserAccessToken}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesgenerator.api.{TriplesGeneratorClient, ProjectUpdates => TGProjectUpdates}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectUpdaterSpec extends AsyncFlatSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "if only GL update needed, " +
    "send update only to GL and TG and succeed on success" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates = projectUpdatesGen
        .map(_.copy(newDescription = None, newKeywords = None))
        .suchThat(_.onlyGLUpdateNeeded)
        .generateOne

      givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = ().asRight.pure[IO])
      givenSendingUpdateToTG(slug, updates, returning = TriplesGeneratorClient.Result.success(()).pure[IO])

      updater.updateProject(slug, updates, authUser).assertNoException
    }

  it should "if core update needed, " +
    "send update to Core, GL and TG and succeed on success" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenBranchProtectionChecking(slug, authUser.accessToken, returning = true.pure[IO])

      val projectGitUrl = projectGitHttpUrls.generateOne
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitUrl.some.pure[IO])
      val userInfo = userInfos.generateOne
      givenUserInfoFinding(authUser.accessToken, returning = userInfo.some.pure[IO])
      val coreUri = coreUrisVersioned.generateOne
      givenFindingCoreUri(projectGitUrl, authUser.accessToken, returning = Result.success(coreUri))

      givenUpdatingProjectInCore(
        coreUri,
        CoreProjectUpdates(projectGitUrl, userInfo, updates.newDescription, updates.newKeywords),
        authUser.accessToken,
        returning = resultSuccesses(()).generateOne
      )
      givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = ().asRight.pure[IO])
      givenSendingUpdateToTG(slug, updates, returning = TriplesGeneratorClient.Result.success(()).pure[IO])

      updater.updateProject(slug, updates, authUser).assertNoException
    }

  it should "if core update needed, " +
    "fail if pushing to the default branch check return false" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenBranchProtectionChecking(slug, authUser.accessToken, returning = false.pure[IO])

      updater.updateProject(slug, updates, authUser).assertThrowsError[Exception](_ shouldBe Failure.cannotPushToBranch)
    }

  it should "if core update needed, " +
    "fail if pushing to the default branch check fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      val exception = exceptions.generateOne
      givenBranchProtectionChecking(slug, authUser.accessToken, returning = exception.raiseError[IO, Nothing])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe Failure.onBranchAccessCheck(slug, authUser.id, exception))
    }

  it should "if core update needed, " +
    "fail if finding project git url fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenBranchProtectionChecking(slug, authUser.accessToken, returning = true.pure[IO])

      val exception = exceptions.generateOne
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = exception.raiseError[IO, Nothing])
      givenUserInfoFinding(authUser.accessToken, returning = userInfos.generateSome.pure[IO])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe Failure.onFindingProjectGitUrl(slug, exception))
    }

  it should "if core update needed, " +
    "fail if finding project git url returns None" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenBranchProtectionChecking(slug, authUser.accessToken, returning = true.pure[IO])

      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = None.pure[IO])
      givenUserInfoFinding(authUser.accessToken, returning = userInfos.generateSome.pure[IO])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe Failure.cannotFindProjectGitUrl)
    }

  it should "if core update needed, " +
    "fail finding user info fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenBranchProtectionChecking(slug, authUser.accessToken, returning = true.pure[IO])
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitHttpUrls.generateSome.pure[IO])
      val exception = exceptions.generateOne
      givenUserInfoFinding(authUser.accessToken, returning = exception.raiseError[IO, Nothing])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe Failure.onFindingUserInfo(authUser.id, exception))
    }

  it should "if core update needed, " +
    "fail finding user info returns None" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenBranchProtectionChecking(slug, authUser.accessToken, returning = true.pure[IO])
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitHttpUrls.generateSome.pure[IO])
      givenUserInfoFinding(authUser.accessToken, returning = None.pure[IO])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe Failure.cannotFindUserInfo(authUser.id))
    }

  it should "if core update needed, " +
    "fail if finding renku core URI fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenBranchProtectionChecking(slug, authUser.accessToken, returning = true.pure[IO])

      val projectGitUrl = projectGitHttpUrls.generateOne
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitUrl.some.pure[IO])
      givenUserInfoFinding(authUser.accessToken, returning = userInfos.generateSome.pure[IO])

      val failedResult = resultDetailedFailures.generateOne
      givenFindingCoreUri(projectGitUrl, authUser.accessToken, returning = failedResult)

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe Failure.onFindingCoreUri(failedResult))
    }

  it should "if core update needed, " +
    "log an error and succeed if updating renku core fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenBranchProtectionChecking(slug, authUser.accessToken, returning = true.pure[IO])

      val projectGitUrl = projectGitHttpUrls.generateOne
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitUrl.some.pure[IO])
      val userInfo = userInfos.generateOne
      givenUserInfoFinding(authUser.accessToken, returning = userInfo.some.pure[IO])
      val coreUri = coreUrisVersioned.generateOne
      givenFindingCoreUri(projectGitUrl, authUser.accessToken, returning = Result.success(coreUri))

      val failedResult = resultDetailedFailures.generateOne
      givenUpdatingProjectInCore(
        coreUri,
        CoreProjectUpdates(projectGitUrl, userInfo, updates.newDescription, updates.newKeywords),
        authUser.accessToken,
        returning = failedResult
      )
      givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = ().asRight.pure[IO])
      givenSendingUpdateToTG(slug, updates, returning = TriplesGeneratorClient.Result.success(()).pure[IO])

      updater.updateProject(slug, updates, authUser).assertNoException >> {
        logger.waitFor(Error(show"Updating project $slug failed", failedResult))
      }
    }

  it should "fail if updating GL returns an error" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates = projectUpdatesGen
      .map(_.copy(newDescription = None, newKeywords = None))
      .suchThat(_.onlyGLUpdateNeeded)
      .generateOne

    val error = Message.Error.fromJsonUnsafe(jsons.generateOne)
    givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = error.asLeft.pure[IO])

    updater
      .updateProject(slug, updates, authUser)
      .assertThrowsError[Exception](_ shouldBe Failure.badRequestOnGLUpdate(error))
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
                             returning = exception.raiseError[IO, Either[Message, Unit]]
    )

    updater
      .updateProject(slug, updates, authUser)
      .assertThrowsError[Exception](_ shouldBe Failure.onGLUpdate(slug, exception))
  }

  it should "fail if updating TG failed" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates = projectUpdatesGen
      .map(_.copy(newDescription = None, newKeywords = None))
      .suchThat(_.onlyGLUpdateNeeded)
      .generateOne

    givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = ().asRight.pure[IO])
    val exception = TriplesGeneratorClient.Result.Failure(exceptions.generateOne.getMessage)
    givenSendingUpdateToTG(slug, updates, returning = exception.pure[IO])

    updater
      .updateProject(slug, updates, authUser)
      .assertThrowsError[Exception](_ shouldBe Failure.onTSUpdate(slug, exception))
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private val branchProtectionCheck = mock[BranchProtectionCheck[IO]]
  private val projectGitUrlFinder   = mock[ProjectGitUrlFinder[IO]]
  private val userInfoFinder        = mock[UserInfoFinder[IO]]
  private val glProjectUpdater      = mock[GLProjectUpdater[IO]]
  private val tgClient              = mock[TriplesGeneratorClient[IO]]
  private val renkuCoreClient       = mock[RenkuCoreClient[IO]]
  private lazy val updater = new ProjectUpdaterImpl[IO](branchProtectionCheck,
                                                        projectGitUrlFinder,
                                                        userInfoFinder,
                                                        glProjectUpdater,
                                                        tgClient,
                                                        renkuCoreClient
  )

  private def givenBranchProtectionChecking(slug: projects.Slug, at: UserAccessToken, returning: IO[Boolean]) =
    (branchProtectionCheck.canPushToDefaultBranch _)
      .expects(slug, at)
      .returning(returning)

  private def givenProjectGitUrlFinding(slug:      projects.Slug,
                                        at:        UserAccessToken,
                                        returning: IO[Option[projects.GitHttpUrl]]
  ) = (projectGitUrlFinder.findGitUrl _)
    .expects(slug, at)
    .returning(returning)

  private def givenUserInfoFinding(at: UserAccessToken, returning: IO[Option[UserInfo]]) =
    (userInfoFinder.findUserInfo _)
      .expects(at)
      .returning(returning)

  private def givenUpdatingProjectInGL(slug:      projects.Slug,
                                       updates:   ProjectUpdates,
                                       at:        UserAccessToken,
                                       returning: IO[Either[Message, Unit]]
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

  private def givenFindingCoreUri(gitUrl:    projects.GitHttpUrl,
                                  at:        UserAccessToken,
                                  returning: Result[RenkuCoreUri.Versioned]
  ) = (renkuCoreClient
    .findCoreUri(_: projects.GitHttpUrl, _: AccessToken))
    .expects(gitUrl, at)
    .returning(returning.pure[IO])

  private def givenUpdatingProjectInCore(coreUri:   RenkuCoreUri.Versioned,
                                         updates:   CoreProjectUpdates,
                                         at:        UserAccessToken,
                                         returning: Result[Unit]
  ) = (renkuCoreClient.updateProject _)
    .expects(coreUri, updates, at)
    .returning(returning.pure[IO])
}
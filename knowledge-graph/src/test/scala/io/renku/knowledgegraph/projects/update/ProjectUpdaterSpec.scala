/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import ProvisioningStatusFinder.ProvisioningStatus
import cats.effect.IO
import cats.syntax.all._
import io.renku.core.client.Generators.{branches, coreUrisVersioned, resultDetailedFailures, resultSuccesses, userInfos}
import io.renku.core.client.{Branch, RenkuCoreClient, RenkuCoreUri, Result, UserInfo, ProjectUpdates => CoreProjectUpdates}
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, jsons}
import io.renku.graph.model.RenkuTinyTypeGenerators.{projectGitHttpUrls, projectSlugs}
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, UserAccessToken}
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.Failure
import io.renku.knowledgegraph.gitlab.UserInfoFinder
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesgenerator.api.Generators.{projectUpdatesGen => tgUpdatesGen}
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

      val glUpdated = glUpdatedProjectsGen.generateSome
      givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = glUpdated.asRight.pure[IO])

      val tgUpdates = tgUpdatesGen.generateOne
      givenTGUpdatesCalculation(updates, glUpdated, returning = tgUpdates.pure[IO])
      givenSendingUpdateToTG(slug, tgUpdates, returning = TriplesGeneratorClient.Result.success(()).pure[IO])

      updater.updateProject(slug, updates, authUser).assertNoException
    }

  it should "if core update needed, " +
    "send update to Core, GL and TG and succeed on success" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne

      val repoBranch    = branches.generateOne
      val defaultBranch = DefaultBranch.Unprotected(repoBranch).some
      givenPrerequisitesCheckFine(slug, authUser.accessToken, defaultBranch.pure[IO])

      val projectGitUrl = projectGitHttpUrls.generateOne
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitUrl.some.pure[IO])
      val userInfo = userInfos.generateOne
      givenUserInfoFinding(authUser.accessToken, returning = userInfo.some.pure[IO])
      val coreUri = coreUrisVersioned.generateOne
      givenFindingCoreUri(projectGitUrl, userInfo, authUser.accessToken, returning = Result.success(coreUri))

      val updates = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne
      givenUpdatingProjectInCore(
        coreUri,
        CoreProjectUpdates(projectGitUrl, userInfo, updates.newDescription, updates.newKeywords),
        authUser.accessToken,
        returning = resultSuccesses(repoBranch).generateOne
      )

      val glUpdated = glUpdatedProjectsGen.generateSome
      givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = glUpdated.asRight.pure[IO])
      val tgUpdates = tgUpdatesGen.generateOne
      givenTGUpdatesCalculation(updates, glUpdated, defaultBranch, repoBranch, returning = tgUpdates.pure[IO])
      givenSendingUpdateToTG(slug, tgUpdates, returning = TriplesGeneratorClient.Result.success(()).pure[IO])

      updater.updateProject(slug, updates, authUser).assertNoException
    }

  it should "if core update needed, " +
    "fail if the project is in unhealthy status from the provisioning point of view" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne

      givenBranchProtectionChecking(slug,
                                    authUser.accessToken,
                                    returning = DefaultBranch.Unprotected(branches.generateOne).some.pure[IO]
      )

      val provisioningStatus = ProvisioningStatus.Unhealthy(EventStatus.GenerationNonRecoverableFailure)
      givenProvisioningStatusFinding(slug, returning = provisioningStatus.pure[IO])

      val updates = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe UpdateFailures.onProvisioningNotHealthy(slug, provisioningStatus))
    }

  it should "if core update needed, " +
    "fail if checking the project status from the provisioning point of view fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne

      givenBranchProtectionChecking(slug,
                                    authUser.accessToken,
                                    returning = DefaultBranch.Unprotected(branches.generateOne).some.pure[IO]
      )

      val exception = exceptions.generateOne
      givenProvisioningStatusFinding(slug, returning = exception.raiseError[IO, Nothing])

      val updates = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe UpdateFailures.onProvisioningStatusCheck(slug, exception))
    }

  it should "if core update needed, " +
    "fail if Core pushed the update to a branch different than the default " +
    "but still update GL and TS" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne

      val defaultBranch = DefaultBranch.PushProtected(branches.generateOne).some
      givenBranchProtectionChecking(slug, authUser.accessToken, returning = defaultBranch.pure[IO])
      givenProvisioningStatusFinding(slug, returning = ProvisioningStatus.Healthy.pure[IO])

      val projectGitUrl = projectGitHttpUrls.generateOne
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitUrl.some.pure[IO])
      val userInfo = userInfos.generateOne
      givenUserInfoFinding(authUser.accessToken, returning = userInfo.some.pure[IO])
      val coreUri = coreUrisVersioned.generateOne
      givenFindingCoreUri(projectGitUrl, userInfo, authUser.accessToken, returning = Result.success(coreUri))

      val updates        = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne
      val corePushBranch = branches.generateOne
      givenUpdatingProjectInCore(
        coreUri,
        CoreProjectUpdates(projectGitUrl, userInfo, updates.newDescription, updates.newKeywords),
        authUser.accessToken,
        returning = resultSuccesses(corePushBranch).generateOne
      )

      val glUpdated = glUpdatedProjectsGen.generateSome
      givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = glUpdated.asRight.pure[IO])
      val tgUpdates = tgUpdatesGen.generateOne
      givenTGUpdatesCalculation(updates, glUpdated, defaultBranch, corePushBranch, returning = tgUpdates.pure[IO])
      givenSendingUpdateToTG(slug, tgUpdates, returning = TriplesGeneratorClient.Result.success(()).pure[IO])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](
          _ shouldBe UpdateFailures.corePushedToNonDefaultBranch(tgUpdates, defaultBranch, corePushBranch)
        )
    }

  it should "if core update needed, " +
    "fail if pushing to the default branch check fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      val exception = exceptions.generateOne
      givenBranchProtectionChecking(slug, authUser.accessToken, returning = exception.raiseError[IO, Nothing])
      givenProvisioningStatusFinding(slug, returning = ProvisioningStatus.Healthy.pure[IO])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe UpdateFailures.onBranchAccessCheck(slug, authUser.id, exception))
    }

  it should "if core update needed, " +
    "fail if finding project git url fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenPrerequisitesCheckFine(slug,
                                  authUser.accessToken,
                                  DefaultBranch.Unprotected(branches.generateOne).some.pure[IO]
      )

      val exception = exceptions.generateOne
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = exception.raiseError[IO, Nothing])
      givenUserInfoFinding(authUser.accessToken, returning = userInfos.generateSome.pure[IO])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe UpdateFailures.onFindingProjectGitUrl(slug, exception))
    }

  it should "if core update needed, " +
    "fail if finding project git url returns None" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenPrerequisitesCheckFine(slug,
                                  authUser.accessToken,
                                  DefaultBranch.Unprotected(branches.generateOne).some.pure[IO]
      )

      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = None.pure[IO])
      givenUserInfoFinding(authUser.accessToken, returning = userInfos.generateSome.pure[IO])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe UpdateFailures.cannotFindProjectGitUrl)
    }

  it should "if core update needed, " +
    "fail finding user info fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenPrerequisitesCheckFine(slug,
                                  authUser.accessToken,
                                  DefaultBranch.Unprotected(branches.generateOne).some.pure[IO]
      )

      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitHttpUrls.generateSome.pure[IO])
      val exception = exceptions.generateOne
      givenUserInfoFinding(authUser.accessToken, returning = exception.raiseError[IO, Nothing])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe UpdateFailures.onFindingUserInfo(authUser.id, exception))
    }

  it should "if core update needed, " +
    "fail finding user info returns None" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenPrerequisitesCheckFine(slug,
                                  authUser.accessToken,
                                  DefaultBranch.Unprotected(branches.generateOne).some.pure[IO]
      )

      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitHttpUrls.generateSome.pure[IO])
      givenUserInfoFinding(authUser.accessToken, returning = None.pure[IO])

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe UpdateFailures.cannotFindUserInfo(authUser.id))
    }

  it should "if core update needed, " +
    "fail if finding renku core URI fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      givenPrerequisitesCheckFine(slug,
                                  authUser.accessToken,
                                  DefaultBranch.Unprotected(branches.generateOne).some.pure[IO]
      )

      val projectGitUrl = projectGitHttpUrls.generateOne
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitUrl.some.pure[IO])
      val userInfo = userInfos.generateOne
      givenUserInfoFinding(authUser.accessToken, returning = userInfo.some.pure[IO])

      val failedResult = resultDetailedFailures.generateOne
      givenFindingCoreUri(projectGitUrl, userInfo, authUser.accessToken, returning = failedResult)

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe UpdateFailures.onFindingCoreUri(failedResult))
    }

  it should "if core update needed, " +
    "fail if updating renku core fails" in {

      val authUser = authUsers.generateOne
      val slug     = projectSlugs.generateOne
      val updates  = projectUpdatesGen.suchThat(_.coreUpdateNeeded).generateOne

      val repoBranch = branches.generateOne
      givenPrerequisitesCheckFine(slug, authUser.accessToken, DefaultBranch.Unprotected(repoBranch).some.pure[IO])

      val projectGitUrl = projectGitHttpUrls.generateOne
      givenProjectGitUrlFinding(slug, authUser.accessToken, returning = projectGitUrl.some.pure[IO])
      val userInfo = userInfos.generateOne
      givenUserInfoFinding(authUser.accessToken, returning = userInfo.some.pure[IO])
      val coreUri = coreUrisVersioned.generateOne
      givenFindingCoreUri(projectGitUrl, userInfo, authUser.accessToken, returning = Result.success(coreUri))

      val failedResult = resultDetailedFailures.generateOne
      givenUpdatingProjectInCore(
        coreUri,
        CoreProjectUpdates(projectGitUrl, userInfo, updates.newDescription, updates.newKeywords),
        authUser.accessToken,
        returning = failedResult
      )

      updater
        .updateProject(slug, updates, authUser)
        .assertThrowsError[Exception](_ shouldBe UpdateFailures.onCoreUpdate(slug, failedResult))
    }

  it should "fail if updating GL returns an error" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates = projectUpdatesGen
      .map(_.copy(newDescription = None, newKeywords = None))
      .suchThat(_.onlyGLUpdateNeeded)
      .generateOne

    val failure = UpdateFailures.badRequestOnGLUpdate(Message.Error.fromJsonUnsafe(jsons.generateOne))
    givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = failure.asLeft.pure[IO])

    updater.updateProject(slug, updates, authUser).assertThrowsError[Exception](_ shouldBe failure)
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
                             returning = exception.raiseError[IO, Either[Failure, Option[GLUpdatedProject]]]
    )

    updater
      .updateProject(slug, updates, authUser)
      .assertThrowsError[Exception](_ shouldBe UpdateFailures.onGLUpdate(slug, exception))
  }

  it should "fail if finding TG updates fails" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates = projectUpdatesGen
      .map(_.copy(newDescription = None, newKeywords = None))
      .suchThat(_.onlyGLUpdateNeeded)
      .generateOne

    val glUpdated = glUpdatedProjectsGen.generateSome
    givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = glUpdated.asRight.pure[IO])

    val exception = TriplesGeneratorClient.Result.Failure(exceptions.generateOne.getMessage)
    givenTGUpdatesCalculation(updates, glUpdated, returning = exception.raiseError[IO, Nothing])

    updater
      .updateProject(slug, updates, authUser)
      .assertThrowsError[Exception](_ shouldBe UpdateFailures.onTGUpdatesFinding(slug, exception))
  }

  it should "fail if updating TG fails" in {

    val authUser = authUsers.generateOne
    val slug     = projectSlugs.generateOne
    val updates = projectUpdatesGen
      .map(_.copy(newDescription = None, newKeywords = None))
      .suchThat(_.onlyGLUpdateNeeded)
      .generateOne

    val glUpdated = glUpdatedProjectsGen.generateSome
    givenUpdatingProjectInGL(slug, updates, authUser.accessToken, returning = glUpdated.asRight.pure[IO])
    val tgUpdates = tgUpdatesGen.generateOne
    givenTGUpdatesCalculation(updates, glUpdated, returning = tgUpdates.pure[IO])
    val exception = TriplesGeneratorClient.Result.Failure(exceptions.generateOne.getMessage)
    givenSendingUpdateToTG(slug, tgUpdates, returning = exception.pure[IO])

    updater
      .updateProject(slug, updates, authUser)
      .assertThrowsError[Exception](_ shouldBe UpdateFailures.onTSUpdate(slug, exception))
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private val provisioningStatusFinder = mock[ProvisioningStatusFinder[IO]]
  private val branchProtectionCheck    = mock[BranchProtectionCheck[IO]]
  private val projectGitUrlFinder      = mock[ProjectGitUrlFinder[IO]]
  private val userInfoFinder           = mock[UserInfoFinder[IO]]
  private val glProjectUpdater         = mock[GLProjectUpdater[IO]]
  private val tgClient                 = mock[TriplesGeneratorClient[IO]]
  private val renkuCoreClient          = mock[RenkuCoreClient[IO]]
  private val tgUpdatesFinder          = mock[TGUpdatesFinder[IO]]
  private lazy val updater = new ProjectUpdaterImpl[IO](provisioningStatusFinder,
                                                        branchProtectionCheck,
                                                        projectGitUrlFinder,
                                                        userInfoFinder,
                                                        glProjectUpdater,
                                                        tgClient,
                                                        renkuCoreClient,
                                                        tgUpdatesFinder
  )

  private def givenPrerequisitesCheckFine(slug:      projects.Slug,
                                          at:        UserAccessToken,
                                          returning: IO[Option[DefaultBranch]]
  ) = {
    givenProvisioningStatusFinding(slug, returning = ProvisioningStatus.Healthy.pure[IO])
    givenBranchProtectionChecking(slug, at, returning = returning)
  }

  private def givenProvisioningStatusFinding(slug: projects.Slug, returning: IO[ProvisioningStatus]) =
    (provisioningStatusFinder.checkHealthy _)
      .expects(slug)
      .returning(returning)

  private def givenBranchProtectionChecking(slug:      projects.Slug,
                                            at:        UserAccessToken,
                                            returning: IO[Option[DefaultBranch]]
  ) =
    (branchProtectionCheck.findDefaultBranchInfo _)
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
                                       returning: IO[Either[Failure, Option[GLUpdatedProject]]]
  ) = (glProjectUpdater.updateProject _)
    .expects(slug, updates, at)
    .returning(returning)

  private def givenSendingUpdateToTG(slug:      projects.Slug,
                                     updates:   TGProjectUpdates,
                                     returning: IO[TriplesGeneratorClient.Result[Unit]]
  ) = (tgClient.updateProject _)
    .expects(slug, updates)
    .returning(returning)

  private def givenFindingCoreUri(gitUrl:    projects.GitHttpUrl,
                                  userInfo:  UserInfo,
                                  at:        UserAccessToken,
                                  returning: Result[RenkuCoreUri.Versioned]
  ) = (renkuCoreClient
    .findCoreUri(_: projects.GitHttpUrl, _: UserInfo, _: AccessToken))
    .expects(gitUrl, userInfo, at)
    .returning(returning.pure[IO])

  private def givenUpdatingProjectInCore(coreUri:   RenkuCoreUri.Versioned,
                                         updates:   CoreProjectUpdates,
                                         at:        UserAccessToken,
                                         returning: Result[Branch]
  ) = (renkuCoreClient.updateProject _)
    .expects(coreUri, updates, at)
    .returning(returning.pure[IO])

  private def givenTGUpdatesCalculation(updates:               ProjectUpdates,
                                        maybeGLUpdatedProject: Option[GLUpdatedProject],
                                        returning:             IO[TGProjectUpdates]
  ) = (tgUpdatesFinder
    .findTGProjectUpdates(_: ProjectUpdates, _: Option[GLUpdatedProject]))
    .expects(updates, maybeGLUpdatedProject)
    .returning(returning)

  private def givenTGUpdatesCalculation(updates:               ProjectUpdates,
                                        maybeGLUpdatedProject: Option[GLUpdatedProject],
                                        maybeDefaultBranch:    Option[DefaultBranch],
                                        corePushBranch:        Branch,
                                        returning:             IO[TGProjectUpdates]
  ) = (tgUpdatesFinder
    .findTGProjectUpdates(_: ProjectUpdates, _: Option[GLUpdatedProject], _: Option[DefaultBranch], _: Branch))
    .expects(updates, maybeGLUpdatedProject, maybeDefaultBranch, corePushBranch)
    .returning(returning)
}

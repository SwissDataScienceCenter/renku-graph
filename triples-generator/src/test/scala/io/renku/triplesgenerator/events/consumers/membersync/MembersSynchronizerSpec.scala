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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects.Path
import io.renku.graph.model.{RenkuUrl, TSVersion, projects}
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder.Implicits.projectPathToPath
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.triplesgenerator.events.consumers.membersync.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class MembersSynchronizerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "synchronizeMembers" should {

    "pulls members from GitLab and sync the members in both versions of TS" in new TestCase {

      val membersInGitLab = gitLabProjectMembers.generateSet()

      val maybeAccessToken = accessTokens.generateOption
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(maybeAccessToken.pure[Try])

      (gitLabProjectMembersFinder
        .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(membersInGitLab.pure[Try])

      val dgSyncSummary = syncSummaries.generateOne
      (defaultGraphSynchronizer.syncMembers _)
        .expects(projectPath, membersInGitLab)
        .returning(dgSyncSummary.pure[Try])
      val ngSyncSummary = syncSummaries.generateOne
      (namedGraphsSynchronizer.syncMembers _)
        .expects(projectPath, membersInGitLab)
        .returning(ngSyncSummary.pure[Try])

      synchronizer.synchronizeMembers(projectPath) shouldBe ().pure[Try]

      logger.loggedOnly(
        infoMessage(dgSyncSummary, TSVersion.DefaultGraph),
        infoMessage(ngSyncSummary, TSVersion.NamedGraphs)
      )
    }

    "recover with log statement if collaborator fails" in new TestCase {

      val maybeAccessToken = accessTokens.generateOption
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(projectPath, projectPathToPath)
        .returning(maybeAccessToken.pure[Try])

      val exception = exceptions.generateOne
      (gitLabProjectMembersFinder
        .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
        .expects(projectPath, maybeAccessToken)
        .returning(exception.raiseError[Try, Set[GitLabProjectMember]])

      synchronizer.synchronizeMembers(projectPath) shouldBe ().pure[Try]

      logger.loggedOnly(
        Error(s"$categoryName: Members synchronized for project $projectPath FAILED", exception)
      )
    }
  }

  private trait TestCase {
    implicit val renkuUrl: RenkuUrl = renkuUrls.generateOne
    val projectPath = projectPaths.generateOne

    implicit val logger:            TestLogger[Try]        = TestLogger[Try]()
    implicit val accessTokenFinder: AccessTokenFinder[Try] = mock[AccessTokenFinder[Try]]
    val gitLabProjectMembersFinder = mock[GitLabProjectMembersFinder[Try]]
    val defaultGraphSynchronizer   = mock[KGSynchronizer[Try]]
    val namedGraphsSynchronizer    = mock[KGSynchronizer[Try]]
    val executionTimeRecorder      = TestExecutionTimeRecorder[Try](maybeHistogram = None)
    val synchronizer = new MembersSynchronizerImpl[Try](gitLabProjectMembersFinder,
                                                        defaultGraphSynchronizer,
                                                        namedGraphsSynchronizer,
                                                        executionTimeRecorder
    )

    def infoMessage(syncSummary: SyncSummary, tsVersion: TSVersion) = Info(
      s"$categoryName: Members for project: $projectPath in $tsVersion synchronized in ${executionTimeRecorder.elapsedTime}ms: " +
        s"${syncSummary.membersAdded} member(s) added, ${syncSummary.membersRemoved} member(s) removed"
    )
  }
}

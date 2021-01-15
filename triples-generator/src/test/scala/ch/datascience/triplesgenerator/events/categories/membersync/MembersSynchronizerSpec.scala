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

package ch.datascience.triplesgenerator.events.categories.membersync

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.{accessTokens, sparqlQueries}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.IOAccessTokenFinder.projectPathToPath
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import ch.datascience.triplesgenerator.events.categories.membersync.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.generators.Generators._

import scala.util.Try

class MembersSynchronizerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "synchronizeMembers" should {
    "pulls members from Gitlab & KG" +
      "AND applies diff in triplestore" in new TestCase {

        val gitLabMemberMissingInKG = gitLabProjectMembers.generateOne
        val gitLabMemberAlsoInKG    = gitLabProjectMembers.generateOne
        val kgMemberAlsoInGitLab    = kgProjectMembers.generateOne.copy(gitLabId = gitLabMemberAlsoInKG.id)
        val kgMemberMissingInGitLab = kgProjectMembers.generateOne

        val membersInGitLab = Set(gitLabMemberMissingInKG, gitLabMemberAlsoInKG)
        val membersInKG     = Set(kgMemberAlsoInGitLab, kgMemberMissingInGitLab)

        implicit val maybeAccessToken = accessTokens.generateOption
        (accessTokenFinder
          .findAccessToken(_: Path)(_: Path => String))
          .expects(projectPath, projectPathToPath)
          .returning(maybeAccessToken.pure[Try])

        (gitLabProjectMembersFinder
          .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
          .expects(projectPath, maybeAccessToken)
          .returning(membersInGitLab.pure[Try])

        (kGProjectMembersFinder
          .findProjectMembers(_: projects.Path))
          .expects(projectPath)
          .returning(membersInKG.pure[Try])

        val removalQuery     = sparqlQueries.generateOne
        val insertionQueries = sparqlQueries.generateNonEmptyList().toList

        (updatesCreator.insertion _)
          .expects(projectPath, Set(gitLabMemberMissingInKG))
          .returning(insertionQueries)

        (updatesCreator.removal _)
          .expects(projectPath, Set(kgMemberMissingInGitLab))
          .returning(removalQuery)

        (removalQuery +: insertionQueries).foreach { query =>
          (querySender.send _)
            .expects(query)
            .returning(().pure[Try])
        }

        synchronizer.synchronizeMembers(projectPath) shouldBe ().pure[Try]

        logger.loggedOnly(Info(s"${EventHandler.categoryName}: Members synchronized for project: $projectPath"))
      }

    "recover with log statement if collaborator fails" in new TestCase {

      implicit val maybeAccessToken = accessTokens.generateOption
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
        Error(s"${EventHandler.categoryName}: Members synchronized for project $projectPath FAILED", exception)
      )
    }

  }

  private trait TestCase {
    val gitLabProjectMembersFinder = mock[GitLabProjectMembersFinder[Try]]
    val kGProjectMembersFinder     = mock[KGProjectMembersFinder[Try]]
    val accessTokenFinder          = mock[AccessTokenFinder[Try]]
    val updatesCreator             = mock[UpdatesCreator]
    val querySender                = mock[QuerySender[Try]]
    val logger                     = TestLogger[Try]()
    val projectPath                = projectPaths.generateOne

    val synchronizer = new MembersSynchronizerImpl[Try](
      accessTokenFinder,
      gitLabProjectMembersFinder,
      kGProjectMembersFinder,
      updatesCreator,
      querySender,
      logger
    )
  }

}

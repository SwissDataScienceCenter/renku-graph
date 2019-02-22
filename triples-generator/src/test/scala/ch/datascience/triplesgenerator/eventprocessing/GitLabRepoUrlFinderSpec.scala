/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing

import cats.MonadError
import cats.implicits._
import ch.datascience.config.ServiceUrl
import ch.datascience.generators.CommonsTypesGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.http.client.AccessToken
import ch.datascience.triplesgenerator.eventprocessing.Commands.GitLabRepoUrlFinder
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class GitLabRepoUrlFinderSpec extends WordSpec with MockFactory {

  "findRepositoryUrl" should {

    protocols foreach { protocol =>
      val host = nonEmptyStrings().generateOne
      val port = positiveInts(9999).generateOne
      val path = projectPaths.generateOne

      s"return '$protocol://$host:$port/$path.git' when no access token" in new TestCase {

        (gitLabUrlProvider.get _)
          .expects()
          .returning(context.pure(ServiceUrl(s"$protocol://$host:$port")))

        val maybeAccessToken = Option.empty[AccessToken]

        urlFinder.findRepositoryUrl(path, maybeAccessToken) shouldBe context.pure(
          ServiceUrl(s"$protocol://$host:$port/$path.git")
        )
      }

      s"return '$protocol://gitlab-ci-token:<token>@$host:$port/$path.git' for personal access token" in new TestCase {

        (gitLabUrlProvider.get _)
          .expects()
          .returning(context.pure(ServiceUrl(s"$protocol://$host:$port")))

        val accessToken = personalAccessTokens.generateOne

        urlFinder.findRepositoryUrl(path, Some(accessToken)) shouldBe context.pure(
          ServiceUrl(s"$protocol://gitlab-ci-token:${accessToken.value}@$host:$port/$path.git")
        )
      }

      s"return '$protocol://oauth2:<token>@$host:$port/$path.git' for personal access token" in new TestCase {

        (gitLabUrlProvider.get _)
          .expects()
          .returning(context.pure(ServiceUrl(s"$protocol://$host:$port")))

        val accessToken = oauthAccessTokens.generateOne

        urlFinder.findRepositoryUrl(path, Some(accessToken)) shouldBe context.pure(
          ServiceUrl(s"$protocol://oauth2:${accessToken.value}@$host:$port/$path.git")
        )
      }
    }

    "fail if finding GitLab url fails" in new TestCase {

      val exception = exceptions.generateOne
      (gitLabUrlProvider.get _)
        .expects()
        .returning(context.raiseError(exception))

      val path             = projectPaths.generateOne
      val maybeAccessToken = Option.empty[AccessToken]

      urlFinder.findRepositoryUrl(path, maybeAccessToken) shouldBe context.raiseError(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val gitLabUrlProvider = mock[TryGitLabUrlProvider]
    val urlFinder         = new GitLabRepoUrlFinder[Try](gitLabUrlProvider)
  }

  private lazy val protocols = Set("http", "https")
}

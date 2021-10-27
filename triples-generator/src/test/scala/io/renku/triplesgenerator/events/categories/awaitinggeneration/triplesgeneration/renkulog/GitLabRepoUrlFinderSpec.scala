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

package io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog

import cats.MonadError
import io.renku.config.ServiceUrl
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.client.AccessToken
import io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog.Commands.{GitLabRepoUrlFinder, GitLabRepoUrlFinderImpl}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class GitLabRepoUrlFinderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "findRepositoryUrl" should {

    protocols foreach { protocol =>
      val host = nonEmptyStrings().generateOne
      val port = positiveInts(9999).generateOne
      val path = projectPaths.generateOne

      s"return '$protocol://$host:$port/$path.git' when no access token" in new TestCase {

        val maybeAccessToken = Option.empty[AccessToken]

        val repoUrlFinder = newRepoUrlFinder(GitLabUrl(s"$protocol://$host:$port"))

        repoUrlFinder.findRepositoryUrl(path, maybeAccessToken) shouldBe context.pure(
          ServiceUrl(s"$protocol://$host:$port/$path.git")
        )
      }

      s"return '$protocol://gitlab-ci-token:<token>@$host:$port/$path.git' for personal access token" in new TestCase {

        val accessToken = personalAccessTokens.generateOne

        val repoUrlFinder = newRepoUrlFinder(GitLabUrl(s"$protocol://$host:$port"))

        repoUrlFinder.findRepositoryUrl(path, Some(accessToken)) shouldBe context.pure(
          ServiceUrl(s"$protocol://gitlab-ci-token:${accessToken.value}@$host:$port/$path.git")
        )
      }

      s"return '$protocol://oauth2:<token>@$host:$port/$path.git' for personal access token" in new TestCase {

        val accessToken = oauthAccessTokens.generateOne

        val repoUrlFinder = newRepoUrlFinder(GitLabUrl(s"$protocol://$host:$port"))

        repoUrlFinder.findRepositoryUrl(path, Some(accessToken)) shouldBe context.pure(
          ServiceUrl(s"$protocol://oauth2:${accessToken.value}@$host:$port/$path.git")
        )
      }
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]
    val newRepoUrlFinder: GitLabUrl => GitLabRepoUrlFinder[Try] = new GitLabRepoUrlFinderImpl[Try](_)
  }

  private lazy val protocols = Set("http", "https")
}

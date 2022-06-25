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

package io.renku.triplesgenerator.events.consumers.awaitinggeneration.triplesgeneration.renkulog

import cats.syntax.all._
import io.renku.config.ServiceUrl
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.client.AccessToken
import io.renku.triplesgenerator.events.consumers.awaitinggeneration.triplesgeneration.renkulog.Commands.{GitLabRepoUrlFinder, GitLabRepoUrlFinderImpl}
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

        implicit val maybeAccessToken: Option[AccessToken] = Option.empty

        val repoUrlFinder = newRepoUrlFinder(GitLabUrl(s"$protocol://$host:$port"))

        repoUrlFinder.findRepositoryUrl(path) shouldBe
          ServiceUrl(s"$protocol://$host:$port/$path.git").pure[Try]
      }

      s"return '$protocol://gitlab-ci-token:<token>@$host:$port/$path.git' for personal access token" in new TestCase {

        implicit val token @ Some(accessToken) = personalAccessTokens.generateSome

        val repoUrlFinder = newRepoUrlFinder(GitLabUrl(s"$protocol://$host:$port"))

        repoUrlFinder.findRepositoryUrl(path) shouldBe
          ServiceUrl(s"$protocol://gitlab-ci-token:${accessToken.value}@$host:$port/$path.git").pure[Try]
      }

      s"return '$protocol://oauth2:<token>@$host:$port/$path.git' for personal access token" in new TestCase {

        implicit val token @ Some(accessToken) = oauthAccessTokens.generateSome

        val repoUrlFinder = newRepoUrlFinder(GitLabUrl(s"$protocol://$host:$port"))

        repoUrlFinder.findRepositoryUrl(path) shouldBe
          ServiceUrl(s"$protocol://oauth2:${accessToken.value}@$host:$port/$path.git").pure[Try]
      }
    }
  }

  private trait TestCase {
    val newRepoUrlFinder: GitLabUrl => GitLabRepoUrlFinder[Try] = new GitLabRepoUrlFinderImpl[Try](_)
  }

  private lazy val protocols = Set("http", "https")
}

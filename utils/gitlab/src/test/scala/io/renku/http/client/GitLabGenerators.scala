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

package io.renku.http.client

import io.renku.generators.Generators
import io.renku.http.client.AccessToken._
import org.scalacheck.Gen

trait GitLabGenerators extends HttpClientGenerators {

  val gitLabUrls: Gen[GitLabUrl] = for {
    url  <- Generators.httpUrls()
    path <- Generators.relativePaths(maxSegments = 2)
  } yield GitLabUrl(s"$url/$path")

  val gitLabApiUrls: Gen[GitLabApiUrl] = gitLabUrls.map(_.apiV4)

  val personalAccessTokens: Gen[PersonalAccessToken] = for {
    length <- Gen.choose(5, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield PersonalAccessToken(chars.mkString(""))

  val userOAuthAccessTokens: Gen[UserOAuthAccessToken] = for {
    length <- Gen.choose(5, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield UserOAuthAccessToken(chars.mkString(""))

  val projectAccessTokens: Gen[ProjectAccessToken] = for {
    chars <- Gen.listOfN(20, Gen.oneOf(('A' to 'Z').map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield ProjectAccessToken(s"$ProjectAccessTokenDefaultPrefix${chars.mkString("")}")

  val userAccessTokens: Gen[UserAccessToken] = Gen.oneOf(userOAuthAccessTokens, personalAccessTokens)

  implicit val accessTokens: Gen[AccessToken] =
    Gen.oneOf(projectAccessTokens, userOAuthAccessTokens, personalAccessTokens)

}

object GitLabGenerators extends GitLabGenerators

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

package ch.datascience.webhookservice.generators

import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.webhookservice.crypto.HookTokenCrypto.HookAuthToken
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.model.{AccessToken, HookToken, ProjectAccessToken}
import ch.datascience.webhookservice.model.AccessToken._
import eu.timepit.refined.api.RefType
import org.scalacheck.Gen

object ServiceTypesGenerators {

  implicit val pushEvents: Gen[PushEvent] = for {
    maybeBefore <- Gen.option(commitIds)
    after       <- commitIds
    pushUser    <- pushUsers
    project     <- projects
  } yield PushEvent(maybeBefore, after, pushUser, project)

  implicit val personalAccessTokens: Gen[PersonalAccessToken] = for {
    length <- Gen.choose(5, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield PersonalAccessToken(chars.mkString(""))

  implicit val oauthAccessTokens: Gen[OAuthAccessToken] = for {
    length <- Gen.choose(5, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield OAuthAccessToken(chars.mkString(""))

  implicit val accessTokens: Gen[AccessToken] = for {
    boolean     <- Gen.oneOf(true, false)
    accessToken <- if (boolean) personalAccessTokens else oauthAccessTokens
  } yield accessToken

  implicit val hookAuthTokens: Gen[HookAuthToken] = nonEmptyStrings().map { value =>
    RefType
      .applyRef[HookAuthToken](value)
      .getOrElse(throw new IllegalArgumentException("Generated HookAuthToken cannot be blank"))
  }

  implicit val projectAccessTokens: Gen[ProjectAccessToken] = for {
    length <- Gen.choose(5, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield ProjectAccessToken(chars.mkString(""))

  implicit val hookTokens: Gen[HookToken] = for {
    projectId          <- projectIds
    projectAccessToken <- projectAccessTokens
  } yield HookToken(projectId, projectAccessToken)
}

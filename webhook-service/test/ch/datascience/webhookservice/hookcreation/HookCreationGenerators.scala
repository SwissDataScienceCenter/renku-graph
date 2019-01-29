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

package ch.datascience.webhookservice.hookcreation

import ch.datascience.generators.Generators.validatedUrls
import ch.datascience.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import ch.datascience.webhookservice.hookcreation.ProjectHookUrlFinder.ProjectHookUrl
import org.scalacheck.Gen
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.hookcreation.LatestPushEventFetcher.PushEventInfo
import ch.datascience.webhookservice.hookcreation.ProjectHookVerifier.HookIdentifier
import ch.datascience.webhookservice.hookcreation.UserInfoFinder.UserInfo

private object HookCreationGenerators {

  implicit val projectHookUrls: Gen[ProjectHookUrl] =
    validatedUrls map (url => ProjectHookUrl.apply(url.value))

  implicit val projectHooks: Gen[ProjectHook] = for {
    projectId           <- projectIds
    hookUrl             <- projectHookUrls
    serializedHookToken <- serializedHookTokens
  } yield
    ProjectHook(
      projectId,
      hookUrl,
      serializedHookToken
    )

  implicit val projectHookIds: Gen[HookIdentifier] = for {
    projectId <- projectIds
    hookUrl   <- projectHookUrls
  } yield
    HookIdentifier(
      projectId,
      hookUrl,
    )

  implicit val pushEventInfos: Gen[PushEventInfo] = for {
    projectId <- projectIds
    userId    <- userIds
    commitTo  <- commitIds
  } yield
    PushEventInfo(
      projectId,
      userId,
      commitTo
    )

  implicit val userInfos: Gen[UserInfo] = for {
    userId   <- userIds
    username <- usernames
    email    <- emails
  } yield
    UserInfo(
      userId,
      username,
      email
    )
}

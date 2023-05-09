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

package io.renku.webhookservice

import eu.timepit.refined.api.Refined
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import io.renku.webhookservice.model._
import org.scalacheck.Gen

object WebhookServiceGenerators {

  implicit val serializedHookTokens: Gen[SerializedHookToken] = nonEmptyStrings() map Refined.unsafeApply

  implicit val hookTokens: Gen[HookToken] =
    projectIds.map(HookToken)

  implicit val selfUrls:        Gen[SelfUrl]        = validatedUrls map (url => SelfUrl.apply(url.value))
  implicit val projectHookUrls: Gen[ProjectHookUrl] = selfUrls map ProjectHookUrl.from

  lazy val hookIdAndUrls: Gen[HookIdAndUrl] = for {
    id      <- positiveInts().map(_.value)
    hookUrl <- projectHookUrls
  } yield HookIdAndUrl(id, hookUrl)

  lazy val projectHookIds: Gen[HookIdentifier] = for {
    projectId <- projectIds
    hookUrl   <- projectHookUrls
  } yield HookIdentifier(projectId, hookUrl)

}

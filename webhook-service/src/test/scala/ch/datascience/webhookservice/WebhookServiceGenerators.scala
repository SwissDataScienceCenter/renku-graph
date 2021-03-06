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

package ch.datascience.webhookservice

import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import ch.datascience.webhookservice.hookcreation.project._
import ch.datascience.webhookservice.model._
import eu.timepit.refined.api.Refined
import org.scalacheck.Gen

object WebhookServiceGenerators {

  implicit val commitSyncRequests: Gen[CommitSyncRequest] = for {
    project <- projects
  } yield CommitSyncRequest(project)

  implicit lazy val projects: Gen[Project] = for {
    projectId <- projectIds
    path      <- projectPaths
  } yield Project(projectId, path)

  implicit val serializedHookTokens: Gen[SerializedHookToken] = nonEmptyStrings() map Refined.unsafeApply

  implicit val hookTokens: Gen[HookToken] = for {
    projectId <- projectIds
  } yield HookToken(projectId)

  implicit val projectInfos: Gen[ProjectInfo] = for {
    id         <- projectIds
    visibility <- projectVisibilities
    path       <- projectPaths
  } yield ProjectInfo(id, visibility, path)

  implicit val selfUrls:        Gen[SelfUrl]        = validatedUrls map (url => SelfUrl.apply(url.value))
  implicit val projectHookUrls: Gen[ProjectHookUrl] = selfUrls map ProjectHookUrl.from
}

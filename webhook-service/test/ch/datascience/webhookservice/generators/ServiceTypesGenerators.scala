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
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.model._
import ch.datascience.webhookservice.project.ProjectHookUrlFinder.ProjectHookUrl
import ch.datascience.webhookservice.project.SelfUrlConfig.SelfUrl
import eu.timepit.refined.api.RefType
import org.scalacheck.Gen

object ServiceTypesGenerators {

  implicit val pushEvents: Gen[PushEvent] = for {
    maybeBefore <- Gen.option(commitIds)
    after       <- commitIds
    pushUser    <- pushUsers
    project     <- projects
  } yield PushEvent(maybeBefore, after, pushUser, project)

  implicit val serializedHookTokens: Gen[SerializedHookToken] = nonEmptyStrings().map { value =>
    RefType
      .applyRef[SerializedHookToken](value)
      .getOrElse(throw new IllegalArgumentException("Generated HookAuthToken cannot be blank"))
  }

  implicit val hookTokens: Gen[HookToken] = for {
    projectId <- projectIds
  } yield HookToken(projectId)

  implicit val projectOwner: Gen[ProjectOwner] = for {
    id <- userIds
  } yield ProjectOwner(id)

  implicit val projectInfos: Gen[ProjectInfo] = for {
    id         <- projectIds
    visibility <- projectVisibilities
    path       <- projectPaths
    owner      <- projectOwner
  } yield ProjectInfo(id, visibility, path, owner)

  implicit val selfUrls: Gen[SelfUrl] =
    validatedUrls map (url => SelfUrl.apply(url.value))

  implicit val projectHookUrls: Gen[ProjectHookUrl] =
    validatedUrls map (url => ProjectHookUrl.apply(url.value))
}

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
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.webhookservice.commits.CommitInfo
import ch.datascience.webhookservice.crypto.HookTokenCrypto.SerializedHookToken
import ch.datascience.webhookservice.eventprocessing.StartCommit
import ch.datascience.webhookservice.model._
import ch.datascience.webhookservice.project.ProjectHookUrlFinder.ProjectHookUrl
import ch.datascience.webhookservice.project.SelfUrlConfigProvider.SelfUrl
import ch.datascience.webhookservice.project._
import eu.timepit.refined.api.RefType
import org.scalacheck.Gen

object WebhookServiceGenerators {

  implicit val startCommits: Gen[StartCommit] = for {
    id      <- commitIds
    project <- projects
  } yield StartCommit(id, project)

  implicit val serializedHookTokens: Gen[SerializedHookToken] = nonEmptyStrings().map { value =>
    RefType
      .applyRef[SerializedHookToken](value)
      .getOrElse(throw new IllegalArgumentException("Generated HookAuthToken cannot be blank"))
  }

  implicit val hookTokens: Gen[HookToken] = for {
    projectId <- projectIds
  } yield HookToken(projectId)

  implicit val projectVisibilities: Gen[ProjectVisibility] = Gen.oneOf(ProjectVisibility.all.toList)

  implicit val projectInfos: Gen[ProjectInfo] = for {
    id         <- projectIds
    visibility <- projectVisibilities
    path       <- projectPaths
  } yield ProjectInfo(id, visibility, path)

  implicit val selfUrls: Gen[SelfUrl] =
    validatedUrls map (url => SelfUrl.apply(url.value))

  implicit val projectHookUrls: Gen[ProjectHookUrl] =
    validatedUrls map (url => ProjectHookUrl.apply(url.value))

  implicit val commitInfos: Gen[CommitInfo] = for {
    id            <- commitIds
    message       <- commitMessages
    committedDate <- committedDates
    author        <- users
    committer     <- users
    parents       <- listOf(commitIds)
  } yield CommitInfo(id, message, committedDate, author, committer, parents)
}

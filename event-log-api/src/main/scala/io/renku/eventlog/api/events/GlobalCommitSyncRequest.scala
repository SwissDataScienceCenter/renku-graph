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

package io.renku.eventlog.api.events

import cats.Show
import cats.syntax.all._
import io.circe.DecodingFailure.Reason.CustomReason
import io.circe.literal._
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.renku.events.CategoryName
import io.renku.events.consumers.Project
import io.renku.graph.model.projects

final case class GlobalCommitSyncRequest(project: Project)

object GlobalCommitSyncRequest {

  val categoryName: CategoryName = CategoryName("GLOBAL_COMMIT_SYNC_REQUEST")

  def apply(projectId: projects.GitLabId, projectSlug: projects.Slug): GlobalCommitSyncRequest =
    GlobalCommitSyncRequest(Project(projectId, projectSlug))

  implicit def dispatcher[F[_]]: Dispatcher[F, GlobalCommitSyncRequest] = Dispatcher.instance(categoryName)

  implicit val encoder: Encoder[GlobalCommitSyncRequest] = Encoder.instance { case GlobalCommitSyncRequest(project) =>
    json"""{
      "categoryName": $categoryName,
      "project":      $project
    }"""
  }

  implicit val decoder: Decoder[GlobalCommitSyncRequest] = Decoder.instance { cursor =>
    lazy val validateCategory = cursor.downField("categoryName").as[CategoryName] >>= {
      case `categoryName` => ().asRight
      case other          => DecodingFailure(CustomReason(s"Expected $categoryName but got $other"), cursor).asLeft
    }

    validateCategory >> cursor.downField("project").as[Project].map(GlobalCommitSyncRequest.apply)
  }

  implicit val show: Show[GlobalCommitSyncRequest] = Project.show.contramap(_.project)
}

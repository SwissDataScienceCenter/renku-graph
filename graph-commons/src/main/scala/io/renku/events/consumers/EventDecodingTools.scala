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

package io.renku.events.consumers

import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure, Json}
import io.renku.events.CategoryName
import io.renku.events.consumers.EventSchedulingResult.UnsupportedEventType
import io.renku.graph.model.events.{CompoundEventId, EventId}
import io.renku.graph.model.projects
import io.renku.json.JsonOps.JsonExt

object EventDecodingTools extends EventDecodingTools

trait EventDecodingTools {

  implicit class JsonOps(override val json: Json) extends JsonExt {

    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val categoryName: Either[EventSchedulingResult, CategoryName] =
      json.hcursor
        .downField("categoryName")
        .as[CategoryName]
        .leftMap(_ => UnsupportedEventType)

    lazy val getProject: Either[DecodingFailure, Project] = json.as[Project]

    lazy val getEventId: Either[DecodingFailure, CompoundEventId] = json.as[CompoundEventId]

    lazy val getProjectPath: Either[DecodingFailure, projects.Path] =
      json.hcursor.downField("project").downField("path").as[projects.Path]

    private implicit val projectDecoder: Decoder[Project] = { implicit cursor =>
      for {
        projectId   <- cursor.downField("project").downField("id").as[projects.GitLabId]
        projectPath <- cursor.downField("project").downField("path").as[projects.Path]
      } yield Project(projectId, projectPath)
    }

    private implicit val eventIdDecoder: Decoder[CompoundEventId] = { implicit cursor =>
      for {
        id        <- cursor.downField("id").as[EventId]
        projectId <- cursor.downField("project").downField("id").as[projects.GitLabId]
      } yield CompoundEventId(id, projectId)
    }
  }
}

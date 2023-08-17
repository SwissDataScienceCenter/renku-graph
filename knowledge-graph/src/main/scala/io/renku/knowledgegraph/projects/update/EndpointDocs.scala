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

package io.renku.knowledgegraph
package projects.update

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.literal._
import io.renku.data.Message
import io.renku.graph.model.projects
import io.renku.knowledgegraph.docs.model.Operation.PATCH
import io.renku.knowledgegraph.docs.model._

object EndpointDocs extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    PATCH(
      "Project Update",
      """|API to update project data.
         |
         |Each of the properties can be either set to a new value or omitted in case there's no new value.
         |
         |The properties that can be updated are:
         |* description - possible values are:
         |  * `null` for removing the current description
         |  * any non-blank String value
         |* image - possible values are:
         |  * `null` for removing the current image
         |  * any relative or absolute link to the image
         |* keywords - an array of String values; an empty array removes all the keywords
         |* visibility - possible values are: `public`, `internal`, `private`
         |
         |In case no properties are set, no data will be changed.
         |""".stripMargin,
      Uri / "projects" / namespace / projectName,
      RequestBody(
        "Properties with new values",
        required = true,
        Contents(
          MediaType.`application/json`(
            Schema.`Object`(properties = Map("visibility" -> Schema.EnumString(projects.Visibility.all.map(_.value)))),
            json"""{
              "description": "a new project description",
              "image":       "image.png",
              "keywords":    ["keyword1", "keyword2"],
              "visibility":  "public|internal|private"
            }"""
          )
        )
      ),
      Status.Accepted -> Response(
        "Update process started",
        Contents(MediaType.`application/json`("Update process started", Message.Info("Update process started")))
      ),
      Status.BadRequest -> Response(
        "Invalid payload",
        Contents(MediaType.`application/json`("Invalid payload", Message.Error("Invalid payload")))
      ),
      Status.Unauthorized -> Response(
        "Unauthorized",
        Contents(MediaType.`application/json`("Invalid token", Message.Info("Unauthorized")))
      ),
      Status.NotFound -> Response(
        "Project not found",
        Contents(MediaType.`application/json`("Reason", Message.Info("Project does not exist")))
      ),
      Status.Conflict -> Response(
        "When the update is not possible due to current project configuration",
        Contents(
          MediaType.`application/json`(
            "Reason",
            Message.Info("Updating project not possible; quite likely the user cannot push to the default branch")
          )
        )
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`application/json`("Reason", Message.Info("Message")))
      )
    )
  )

  private lazy val namespace = Parameter.Path(
    "namespace",
    Schema.String,
    description =
      "Namespace(s) as there might be multiple. Each namespace needs to be url-encoded and separated with a non url-encoded '/'".some
  )

  private lazy val projectName = Parameter.Path("projectName", Schema.String, "Project name".some)
}

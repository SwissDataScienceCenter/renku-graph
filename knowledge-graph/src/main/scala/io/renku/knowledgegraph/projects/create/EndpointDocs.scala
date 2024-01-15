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

package io.renku.knowledgegraph
package projects.create

import eu.timepit.refined.auto._
import io.circe.literal._
import io.renku.data.Message
import io.renku.knowledgegraph.docs.model.Operation.POST
import io.renku.knowledgegraph.docs.model._

object EndpointDocs extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    POST(
      "Project Create",
      """|API to create a new project from the given payload in both the Triples Store and GitLab
         |
         |All the properties except from the `description` and `image` are mandatory.
         |The `image` property can be any image file with the limit of 200kB in size and media type one of: `image/png`, `image/jpeg`, `image/gif`, `image/bmp`, `image/tiff`, `image/vnd.microsoft.icon`.
         |The allowed values for `visibility` are: `public`, `internal`, `private`.
         |""".stripMargin,
      Uri / "projects",
      RequestBody(
        "New Project properties",
        required = true,
        Contents(
          MediaType.`multipart/form-data`(
            "Multipart request example",
            """|POST /knowledge-graph/projects HTTP/1.1
               |Host: dev.renku.ch
               |Authorization: Bearer <XXX>
               |Content-Length: 575
               |Content-Type: multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW
               |
               |------WebKitFormBoundary7MA4YWxkTrZu0gW
               |Content-Disposition: form-data; name="name"
               |
               |project name
               |------WebKitFormBoundary7MA4YWxkTrZu0gW
               |Content-Disposition: form-data; name="namespaceId"
               |
               |15
               |------WebKitFormBoundary7MA4YWxkTrZu0gW
               |Content-Disposition: form-data; name="description"
               |
               |project description
               |------WebKitFormBoundary7MA4YWxkTrZu0gW
               |Content-Disposition: form-data; name="keywords[]"
               |
               |key1
               |------WebKitFormBoundary7MA4YWxkTrZu0gW
               |Content-Disposition: form-data; name="keywords[]"
               |
               |key2
               |------WebKitFormBoundary7MA4YWxkTrZu0gW
               |Content-Disposition: form-data; name="visibility"
               |
               |public
               |------WebKitFormBoundary7MA4YWxkTrZu0gW
               |Content-Disposition: form-data; name="templateRepositoryUrl"
               |
               |https://github.com/SwissDataScienceCenter/renku-project-template
               |------WebKitFormBoundary7MA4YWxkTrZu0gW
               |Content-Disposition: form-data; name="templateId"
               |
               |python-minimal
               |------WebKitFormBoundary7MA4YWxkTrZu0gW
               |Content-Disposition: form-data; name="templateRef"
               |
               |0.6.0
               |------WebKitFormBoundary7MA4YWxkTrZu0gW
               |Content-Disposition: form-data; name="image"; filename="image.png"
               |Content-Type: image/png
               |
               |(data)
               |------WebKitFormBoundary7MA4YWxkTrZu0gW--
               |""".stripMargin
          )
        )
      ),
      Status.Created -> Response(
        "Project created",
        Contents(
          MediaType.`application/json`(
            "Project created",
            Message.Info.fromJsonUnsafe {
              json"""{
                "message": "Project created",
                "slug":    "namespace/path"
              }"""
            }
          )
        )
      ),
      Status.BadRequest -> Response(
        "Invalid payload",
        Contents(MediaType.`application/json`("Invalid payload", Message.Error("Invalid payload")))
      ),
      Status.Unauthorized -> Response(
        "Unauthorized",
        Contents(MediaType.`application/json`("Invalid token", Message.Info("Unauthorized")))
      ),
      Status.Forbidden -> Response(
        "Forbidden",
        Contents(MediaType.`application/json`("User not authorized to create the project", Message.Info("Forbidden")))
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`application/json`("Reason", Message.Info("Message")))
      )
    )
  )
}

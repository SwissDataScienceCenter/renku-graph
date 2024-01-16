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

package io.renku.knowledgegraph.version

import eu.timepit.refined.auto._
import io.circe.syntax._
import io.renku.config.{ServiceName, ServiceVersion}
import io.renku.data.Message
import io.renku.http.server.version.Endpoint.encoder
import io.renku.knowledgegraph.docs
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

trait EndpointDocs {
  def path: Path
}

object EndpointDocs extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    GET(
      "Version",
      "Returns info about service version",
      Uri / "version",
      Status.Ok -> Response(
        "Info about service version",
        Contents(
          MediaType.`application/json`("Sample response",
                                       (ServiceName("knowledge-graph") -> ServiceVersion("2.43.0")).asJson
          )
        )
      ),
      Status.InternalServerError ->
        Response("Error", Contents(MediaType.`application/json`("Reason", Message.Error("Message"))))
    )
  )
}

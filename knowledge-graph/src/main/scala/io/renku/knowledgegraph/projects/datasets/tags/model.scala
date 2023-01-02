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

package io.renku.knowledgegraph.projects.datasets.tags

import io.renku.config.renku
import io.renku.graph.model.{datasets, publicationEvents}
import io.renku.knowledgegraph

private object model {

  final case class Tag(name:      publicationEvents.Name,
                       startDate: publicationEvents.StartDate,
                       maybeDesc: Option[publicationEvents.Description],
                       datasetId: datasets.Identifier
  )

  object Tag {

    import io.circe.Encoder

    implicit def modelEncoder(implicit renkuApiUrl: renku.ApiUrl): Encoder[model.Tag] = Encoder.instance { tag =>
      import io.circe.literal._
      import io.renku.http.rest.Links._
      import io.renku.json.JsonOps._

      json"""{
        "name": ${tag.name},
        "date": ${tag.startDate}
      }"""
        .addIfDefined("description" -> tag.maybeDesc)
        .deepMerge(
          _links(
            Link(
              Rel("dataset-details") ->
                knowledgegraph.datasets.details.Endpoint.href(renkuApiUrl, tag.datasetId)
            )
          )
        )
    }
  }
}

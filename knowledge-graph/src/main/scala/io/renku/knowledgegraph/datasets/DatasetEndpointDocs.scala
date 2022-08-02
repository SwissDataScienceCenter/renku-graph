/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.datasets

import Dataset._
import cats.syntax.all._
import io.circe.syntax._
import io.renku.config.renku
import io.renku.graph.model._
import io.renku.http.InfoMessage
import io.renku.http.InfoMessage._
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

import java.time.Instant

trait DatasetEndpointDocs {
  def path: Path
}

private class DatasetEndpointDocsImpl()(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl)
    extends DatasetEndpointDocs {

  override lazy val path: Path = Path(
    "Dataset details",
    "Finds Datasets details".some,
    GET(
      Uri / "entities" / identifier,
      Status.Ok -> Response("Dataset details", Contents(MediaType.`application/json`("Sample response", example))),
      Status.Unauthorized -> Response(
        "Unauthorized",
        Contents(MediaType.`application/json`("Invalid token", InfoMessage("Unauthorized")))
      ),
      Status.NotFound -> Response(
        "In case the dataset cannot be found or use has no privileges to see its details",
        Contents(MediaType.`application/json`("Reason", InfoMessage("Not found")))
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`application/json`("Reason", InfoMessage("Message")))
      )
    )
  )

  private lazy val identifier = Parameter.in(
    "identifier",
    Schema.String,
    "to filter by matching field (e.g., title, keyword, description, or creator name)".some
  )

  private lazy val example = (Dataset
    .NonModifiedDataset(
      datasets.ResourceId("http://renku/datasets/123444"),
      datasets.Identifier("123444"),
      datasets.Title("title"),
      datasets.Name("name"),
      datasets.SameAs("http://datasets-repo/abcd"),
      DatasetVersions(datasets.OriginalIdentifier("12333")),
      datasets.Description("Dataset description").some,
      List(
        DatasetCreator(persons.Email("jan@mail.com").some,
                       persons.Name("Jan Kowalski"),
                       persons.Affiliation("SDSC").some
        )
      ),
      datasets.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
      List(DatasetPart(datasets.PartLocation("data"))),
      DatasetProject(projects.Path("group/subgroup/name"), projects.Name("name")),
      List(DatasetProject(projects.Path("group/subgroup/name"), projects.Name("name"))),
      List(datasets.Keyword("key")),
      List(datasets.ImageUri("image.png"))
    ): Dataset).asJson
}

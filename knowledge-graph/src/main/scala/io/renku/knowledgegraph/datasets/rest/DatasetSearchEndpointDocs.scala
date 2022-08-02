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

package io.renku.knowledgegraph.datasets.rest

import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import io.renku.config.renku
import io.renku.graph.model._
import io.renku.http.InfoMessage
import io.renku.http.InfoMessage._
import io.renku.knowledgegraph.datasets.model.DatasetCreator
import io.renku.knowledgegraph.datasets.rest.DatasetSearchResult._
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

import java.time.Instant

trait DatasetSearchEndpointDocs {
  def path: Path
}

private class DatasetSearchEndpointDocsImpl()(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl)
    extends DatasetSearchEndpointDocs {

  override lazy val path: Path = Path(
    "Free-Text Dataset search",
    "Finds Datasets by the given criteria".some,
    GET(
      Uri / "entities" / query / sort / page / perPage,
      Status.Ok -> Response("Found datasets",
                            Contents(MediaType.`application/json`("Sample response", example)),
                            responseHeaders
      ),
      Status.BadRequest -> Response(
        "In case of invalid query parameters",
        Contents(MediaType.`application/json`("Reason", InfoMessage("Invalid parameters")))
      ),
      Status.Unauthorized -> Response(
        "Unauthorized",
        Contents(MediaType.`application/json`("Invalid token", InfoMessage("Unauthorized")))
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`application/json`("Reason", InfoMessage("Message")))
      )
    )
  )

  private lazy val query = Parameter(
    "query",
    In.Query,
    "to filter by matching field (e.g., title, keyword, description, or creator name)".some,
    required = false,
    Schema.String
  )
  private lazy val sort = Parameter(
    "sort",
    In.Query,
    "the `sort` query parameter is optional and defaults to `title:asc`. Allowed property names are: `title`, `datePublished`, `date` and `projectsCount`".some,
    required = false,
    Schema.String
  )
  private lazy val page = Parameter("page",
                                    In.Query,
                                    "the page query parameter is optional and defaults to 1.".some,
                                    required = false,
                                    Schema.String
  )
  private lazy val perPage = Parameter(
    "per_page",
    In.Query,
    "the per_page query parameter is optional and defaults to 20; max value is 100.".some,
    required = false,
    Schema.String
  )

  private lazy val responseHeaders = Map(
    "Total"       -> Header("The total number of entities".some, Schema.Integer),
    "Total-Pages" -> Header("The total number of pages".some, Schema.Integer),
    "Per-Page"    -> Header("The number of items per page".some, Schema.Integer),
    "Page"        -> Header("The index of the current page (starting at 1)".some, Schema.Integer),
    "Next-Page"   -> Header("The index of the next page (optional)".some, Schema.Integer),
    "Prev-Page"   -> Header("The index of the previous page (optional)".some, Schema.Integer),
    "Link" -> Header("The set of prev/next/first/last link headers (prev and next are optional)".some, Schema.String)
  )

  private lazy val example = Json.arr(
    DatasetSearchResult(
      datasets.Identifier("123444"),
      datasets.Title("title"),
      datasets.Name("name"),
      datasets.Description("Some project").some,
      List(
        DatasetCreator(persons.Email("jan@mail.com").some,
                       persons.Name("Jan Kowalski"),
                       persons.Affiliation("SDSC").some
        )
      ),
      datasets.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
      projects.Path("group/subgroup/name"),
      ProjectsCount(1),
      List(datasets.Keyword("key")),
      List(datasets.ImageUri("image.png"))
    ).asJson
  )
}

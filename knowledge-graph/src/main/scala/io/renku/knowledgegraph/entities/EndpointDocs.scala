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

package io.renku.knowledgegraph.entities

import cats.MonadThrow
import cats.implicits._
import io.circe.Json
import io.circe.syntax._
import io.renku.config.renku
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model._
import io.renku.http.InfoMessage
import io.renku.http.InfoMessage._
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._
import io.renku.knowledgegraph.entities.model.Entity._
import io.renku.knowledgegraph.entities.model.MatchingScore

import java.time.Instant

trait EndpointDocs {
  def path: Path
}

object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[EndpointDocs] = for {
    gitLabUrl <- GitLabUrlLoader[F]()
    apiUrl    <- renku.ApiUrl[F]()
  } yield new EndpointDocsImpl()(gitLabUrl, apiUrl)
}

private class EndpointDocsImpl()(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl) extends EndpointDocs {

  override lazy val path: Path = Path(
    "Cross-Entity search",
    "Finds entities by the given criteria".some,
    GET(
      Uri / "entities" / query / `type` / creator / visibility / since / until / sort / page / perPage,
      Status.Ok -> Response("Found entities",
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

  private lazy val query = Parameter("query",
                                     In.Query,
                                     "to filter by matching field (e.g., title, keyword, description, etc.)".some,
                                     required = false,
                                     Schema.String
  )
  private lazy val `type` = Parameter(
    "type",
    In.Query,
    "to filter by entity type(s); allowed values: project, dataset, workflow, and person; multiple type parameters allowed".some,
    required = false,
    Schema.String
  )
  private lazy val creator = Parameter(
    "creator",
    In.Query,
    "to filter by creator(s); the filter would require creator's name; multiple creator parameters allowed".some,
    required = false,
    Schema.String
  )
  private lazy val visibility = Parameter(
    "visibility",
    In.Query,
    "to filter by visibility(ies) (restricted vs. public); allowed values: public, internal, private; multiple visibility parameters allowed".some,
    required = false,
    Schema.String
  )
  private lazy val since = Parameter("since",
                                     In.Query,
                                     "to filter by entity's creation date to >= the given date".some,
                                     required = false,
                                     Schema.String
  )
  private lazy val until = Parameter("until",
                                     In.Query,
                                     "to filter by entity's creation date to <= the given date".some,
                                     required = false,
                                     Schema.String
  )
  private lazy val sort = Parameter(
    "sort",
    In.Query,
    "the `sort` query parameter is optional and defaults to `name:asc`. Allowed property names are: `matchingScore`, `name` and `date`".some,
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
    Project(
      MatchingScore(1),
      projects.Path("group/subgroup/name"),
      projects.Name("name"),
      projects.Visibility.Public,
      projects.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
      persons.Name("Jan Kowalski").some,
      List(projects.Keyword("key")),
      projects.Description("Some project").some
    ).asJson,
    Dataset(
      MatchingScore(1),
      datasets.Identifier("123444"),
      datasets.Name("name"),
      projects.Visibility.Public,
      datasets.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
      List(persons.Name("Jan Kowalski")),
      List(datasets.Keyword("key")),
      datasets.Description("Some project").some,
      List(datasets.ImageUri("image.png")),
      projects.Path("group/subgroup/name")
    ).asJson,
    Workflow(
      MatchingScore(1),
      plans.Name("name"),
      projects.Visibility.Public,
      plans.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
      List(plans.Keyword("key")),
      plans.Description("description").some
    ).asJson,
    Person(
      MatchingScore(1),
      persons.Name("name")
    ).asJson
  )
}

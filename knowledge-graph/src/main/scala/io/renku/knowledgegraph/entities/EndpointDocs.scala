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

package io.renku.knowledgegraph.entities

import cats.MonadThrow
import cats.implicits._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax._
import io.renku.config.renku
import io.renku.data.Message
import io.renku.data.Message.Codecs._
import io.renku.entities.search.model.Entity._
import io.renku.entities.search.model.MatchingScore
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model._
import io.renku.graph.model.images.ImageUri
import io.renku.knowledgegraph.docs
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._
import io.renku.knowledgegraph.entities.ModelEncoders._

import java.time.Instant

object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[docs.EndpointDocs] = for {
    gitLabUrl <- GitLabUrlLoader[F]()
    apiUrl    <- renku.ApiUrl[F]()
  } yield new EndpointDocsImpl()(gitLabUrl, apiUrl)
}

private class EndpointDocsImpl()(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl) extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    GET(
      "Cross-Entity Search",
      "Finds entities by the given criteria",
      Uri / "entities" :? query & `type` & creator & visibility & namespace & since & until & sort & page & perPage,
      Status.Ok -> Response("Found entities",
                            Contents(MediaType.`application/json`("Sample response", example)),
                            responseHeaders
      ),
      Status.BadRequest -> Response(
        "In case of invalid query parameters",
        Contents(MediaType.`application/json`("Reason", Message.Info("Invalid parameters")))
      ),
      Status.Unauthorized -> Response(
        "Unauthorized",
        Contents(MediaType.`application/json`("Invalid token", Message.Info("Unauthorized")))
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`application/json`("Reason", Message.Info("Message")))
      )
    )
  )

  private lazy val query = Parameter.Query(
    "query",
    Schema.String,
    "to filter by matching value in name/title, namespace, creator, keyword and description".some,
    required = false
  )
  private lazy val `type` = Parameter.Query(
    "type",
    Schema.String,
    "to filter by entity type(s); allowed values: project, dataset, workflow, and person; multiple type parameters allowed".some,
    required = false
  )
  private lazy val creator = Parameter.Query(
    "creator",
    Schema.String,
    "to filter by creator(s); the filter would require creator's name; multiple creator parameters allowed".some,
    required = false
  )
  private lazy val visibility = Parameter.Query(
    "visibility",
    Schema.String,
    "to filter by visibility(ies) (restricted vs. public); allowed values: 'public', 'internal', 'private'; multiple visibility parameters allowed".some,
    required = false
  )
  private lazy val namespace = Parameter.Query(
    "namespace",
    Schema.String,
    "to filter by namespace(s); there might be multiple values given; for nested namespaces the whole path has be used, e.g. 'group/subgroup'".some,
    required = false
  )
  private lazy val since = Parameter.Query(
    "since",
    Schema.String,
    "to filter by entity's creation date to >= the given date".some,
    required = false
  )
  private lazy val until = Parameter.Query(
    "until",
    Schema.String,
    "to filter by entity's creation date to <= the given date".some,
    required = false
  )
  private lazy val sort = Parameter.Query(
    "sort",
    Schema.String,
    "the `sort` query parameter is optional and defaults to `name:asc`. Allowed property names are: `matchingScore`, `name` and `date`".some,
    required = false
  )
  private lazy val page = Parameter.Query(
    "page",
    Schema.Integer,
    "the page query parameter is optional and defaults to 1.".some,
    required = false
  )
  private lazy val perPage = Parameter.Query(
    "per_page",
    Schema.Integer,
    "the per_page query parameter is optional and defaults to 20; max value is 100.".some,
    required = false
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
      projects.DateModified(Instant.parse("2012-11-16T10:00:00.000Z")),
      persons.Name("Jan Kowalski").some,
      List(projects.Keyword("key")),
      projects.Description("Some project").some,
      List(ImageUri("image.png"))
    ).asJson,
    Dataset(
      MatchingScore(1),
      datasets.TopmostSameAs("http://localhost/123444"),
      datasets.Name("name"),
      projects.Visibility.Public,
      datasets.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
      List(persons.Name("Jan Kowalski")),
      List(datasets.Keyword("key")),
      datasets.Description("Some project").some,
      List(ImageUri("image.png")),
      projects.Path("group/subgroup/name")
    ).asJson,
    Workflow(
      MatchingScore(1),
      plans.Name("name"),
      projects.Visibility.Public,
      plans.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
      List(plans.Keyword("key")),
      plans.Description("description").some,
      Workflow.WorkflowType.Step
    ).asJson,
    Person(
      MatchingScore(1),
      persons.Name("name")
    ).asJson
  )
}

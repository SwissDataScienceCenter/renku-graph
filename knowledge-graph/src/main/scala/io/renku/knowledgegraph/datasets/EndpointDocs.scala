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
package datasets

import DatasetSearchResult._
import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax._
import io.renku.config.renku
import io.renku.data.Message
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model._
import io.renku.graph.model.images.ImageUri
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

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
      "Free-Text Dataset Search",
      "Finds Datasets by the given criteria",
      Uri / "datasets" :? query & sort & page & perPage,
      Status.Ok -> Response("Found datasets",
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
    "to filter by matching field (e.g., title, keyword, description, or creator name)".some,
    required = false
  )
  private lazy val sort = Parameter.Query(
    "sort",
    Schema.String,
    "the `sort` query parameter is optional and defaults to `title:asc`. Allowed property names are: `title`, `datePublished`, `date` and `projectsCount`".some,
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

  private lazy val example = {
    implicit val renkuUrl: RenkuUrl = RenkuUrl("http://renku")
    val projectSlug = projects.Slug("group/subgroup/name")

    Json.arr(
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
        ExemplarProject(projects.ResourceId(projectSlug), projectSlug),
        ProjectsCount(1),
        List(datasets.Keyword("key")),
        List(ImageUri("image.png"))
      ).asJson
    )
  }
}

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
package projects.datasets

import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.auto._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.data.Message
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.{datasets, projects}
import io.renku.http.client.{GitLabClientLoader, GitLabUrl}
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

import java.time.{Duration, Instant, LocalDate}

object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[docs.EndpointDocs] = for {
    gitLabUrl <- GitLabClientLoader.gitLabUrl[F](ConfigFactory.load())
    apiUrl    <- renku.ApiUrl[F]()
  } yield new EndpointDocsImpl()(gitLabUrl, apiUrl)
}

private class EndpointDocsImpl()(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl) extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    GET(
      "Project Datasets",
      "Finds Project's Datasets",
      Uri / "projects" / namespace / projectName / "datasets" :? sort & page & perPage,
      Status.Ok -> Response("Datasets found",
                            Contents(MediaType.`application/json`("Sample data", example)),
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
      Status.NotFound -> Response(
        "Project not found or no privileges",
        Contents(
          MediaType.`application/json`("Reason", Message.Info("No namespace/project datasets found"))
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

  private lazy val sort = Parameter.Query(
    "sort",
    Schema.String,
    "the `sort` query parameter is optional and defaults to `name:asc`. Allowed property names are: `name` and `dateModified`. It's also allowed to specify multiple sort parameters".some,
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
    "Total"       -> Header("The total number of projects".some, Schema.Integer),
    "Total-Pages" -> Header("The total number of pages".some, Schema.Integer),
    "Per-Page"    -> Header("The number of items per page".some, Schema.Integer),
    "Page"        -> Header("The index of the current page (starting at 1)".some, Schema.Integer),
    "Next-Page"   -> Header("The index of the next page (optional)".some, Schema.Integer),
    "Prev-Page"   -> Header("The index of the previous page (optional)".some, Schema.Integer),
    "Link" -> Header("The set of prev/next/first/last link headers (prev and next are optional)".some, Schema.String)
  )

  private val example = {
    implicit val dsEncoder: Encoder[ProjectDataset] = ProjectDataset.encoder(projects.Slug("namespace/name"))
    Json.arr(
      ProjectDataset(
        datasets.Identifier("123"),
        datasets.OriginalIdentifier("123"),
        datasets.Name("name"),
        datasets.Slug("slug"),
        datasets.DateCreated(Instant.now().minus(Duration.ofDays(20))),
        maybeDateModified = None,
        datasets.SameAs("http://datasets-repo/abcd").asLeft[datasets.DerivedFrom],
        List(ImageUri("image.png"))
      ).asJson,
      ProjectDataset(
        datasets.Identifier("123"),
        datasets.OriginalIdentifier("123"),
        datasets.Name("name"),
        datasets.Slug("slug"),
        datasets.DatePublished(LocalDate.now().minusDays(20)),
        datasets.DateModified(Instant.now()).some,
        datasets.DerivedFrom("http://datasets-repo/abcd").asRight[datasets.SameAs],
        List(ImageUri("image.png"))
      ).asJson
    )
  }
}

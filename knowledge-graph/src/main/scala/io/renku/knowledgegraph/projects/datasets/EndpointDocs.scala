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
package projects.datasets

import ProjectDatasetEncoder.encoder
import ProjectDatasetsFinder.ProjectDataset
import cats.MonadThrow
import cats.syntax.all._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.{GitLabUrl, datasets, projects}
import io.renku.http.InfoMessage
import io.renku.http.InfoMessage._
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[docs.EndpointDocs] = for {
    gitLabUrl <- GitLabUrlLoader[F]()
    apiUrl    <- renku.ApiUrl[F]()
  } yield new EndpointDocsImpl()(gitLabUrl, apiUrl)
}

private class EndpointDocsImpl()(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl) extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    "Project Datasets",
    "Finds Project Datasets".some,
    GET(
      Uri / "projects" / namespace / projectName / "datasets",
      Status.Ok -> Response("Datasets found", Contents(MediaType.`application/json`("Sample data", example))),
      Status.Unauthorized -> Response(
        "Unauthorized",
        Contents(MediaType.`application/json`("Invalid token", InfoMessage("Unauthorized")))
      ),
      Status.NotFound -> Response(
        "Project not found or no privileges",
        Contents(
          MediaType.`application/json`("Reason", InfoMessage("No namespace/project datasets found"))
        )
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`application/json`("Reason", InfoMessage("Message")))
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

  private val example = {
    implicit val dsEncoder: Encoder[ProjectDataset] = encoder(projects.Path("namespace/name"))
    Json.arr(
      (datasets.Identifier("123"),
       datasets.OriginalIdentifier("123"),
       datasets.Title("dataset"),
       datasets.Name("dataset"),
       datasets.SameAs("http://datasets-repo/abcd").asLeft[datasets.DerivedFrom],
       List(ImageUri("image.png"))
      ).asJson,
      (datasets.Identifier("123"),
       datasets.OriginalIdentifier("123"),
       datasets.Title("dataset"),
       datasets.Name("dataset"),
       datasets.DerivedFrom("http://datasets-repo/abcd").asRight[datasets.SameAs],
       List(ImageUri("image.png"))
      ).asJson
    )
  }
}

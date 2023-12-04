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
package details

import Dataset._
import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.syntax._
import io.renku.config.renku
import io.renku.data.Message
import io.renku.graph.config.{GitLabUrlLoader, RenkuUrlLoader}
import io.renku.graph.model._
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects.Visibility
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

import java.time.Instant

object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[docs.EndpointDocs] = for {
    implicit0(gitLabUrl: GitLabUrl) <- GitLabUrlLoader[F]()
    implicit0(apiUrl: renku.ApiUrl) <- renku.ApiUrl[F]()
    implicit0(renkuUrl: RenkuUrl)   <- RenkuUrlLoader[F]()
  } yield new EndpointDocsImpl
}

private class EndpointDocsImpl(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl, renkuUrl: RenkuUrl)
    extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    GET(
      "Dataset Details",
      "Finds Datasets details",
      Uri / "datasets" / identifier,
      Status.Ok -> Response("Dataset details", Contents(MediaType.`application/json`("Sample response", example))),
      Status.Unauthorized -> Response(
        "Unauthorized",
        Contents(MediaType.`application/json`("Invalid token", Message.Info("Unauthorized")))
      ),
      Status.NotFound -> Response(
        "In case the dataset cannot be found or use has no privileges to see its details",
        Contents(MediaType.`application/json`("Reason", Message.Info("Not found")))
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`application/json`("Reason", Message.Info("Message")))
      )
    )
  )

  private lazy val identifier = Parameter.Path("identifier", Schema.String, "Dataset identifier".some)

  private lazy val example = {
    val sameAs      = datasets.SameAs("http://datasets-repo/abcd")
    val projectSlug = projects.Slug("group/subgroup/name")
    Dataset
      .NonModifiedDataset(
        datasets.ResourceId((renkuUrl / "datasets" / "123444").show),
        datasets.Name("name"),
        datasets.Slug("slug"),
        sameAs,
        DatasetVersions(datasets.OriginalIdentifier("12333")),
        Tag(publicationEvents.Name("2.0"), publicationEvents.Description("Tag Dataset was imported from").some).some,
        datasets.Description("Dataset description").some,
        List(
          DatasetCreator(persons.Email("jan@mail.com").some,
                         persons.Name("Jan Kowalski"),
                         persons.Affiliation("SDSC").some
          )
        ),
        datasets.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
        List(DatasetPart(datasets.PartLocation("data"))),
        project = DatasetProject(projects.ResourceId(projectSlug),
                                 projectSlug,
                                 projects.Name("name"),
                                 Visibility.Public,
                                 datasets.Identifier("123444")
        ),
        usedIn = List(
          DatasetProject(projects.ResourceId(projectSlug),
                         projectSlug,
                         projects.Name("name"),
                         Visibility.Public,
                         datasets.Identifier("123444")
          )
        ),
        List(datasets.Keyword("key")),
        List(ImageUri("image.png"))
      )
      .widen
      .asJson(Dataset.encoder(RequestedDataset(sameAs)))
  }
}

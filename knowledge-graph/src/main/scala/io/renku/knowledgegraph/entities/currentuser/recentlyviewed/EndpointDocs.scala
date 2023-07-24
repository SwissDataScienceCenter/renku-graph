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

package io.renku.knowledgegraph.entities.currentuser.recentlyviewed

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

final class EndpointDocsImpl()(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl) extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    GET(
      "User Recently Viewed Entities",
      "Finds entities the user recently viewed",
      Uri / "current-user" / "recently-viewed" :? limit :? entityType,
      Status.Ok -> Response("Found entities", Contents(MediaType.`application/json`("Sample response", example))),
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

  private lazy val limit = Parameter.Query(
    "limit",
    Schema.Integer,
    "Limit the results by this amount. Must be > 0 and <= 200. Defaults to 10".some,
    required = false
  )

  private lazy val entityType = Parameter.Query(
    "type",
    Schema.String,
    "One of 'project' or 'dataset'. Can be given multiple times. If non-existent, all types are returned.".some,
    required = false
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
    ).asJson
  )
}

object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[docs.EndpointDocs] = for {
    gitLabUrl <- GitLabUrlLoader[F]()
    apiUrl    <- renku.ApiUrl[F]()
  } yield new EndpointDocsImpl()(gitLabUrl, apiUrl)
}

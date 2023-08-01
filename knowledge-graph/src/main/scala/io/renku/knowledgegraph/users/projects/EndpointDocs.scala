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

package io.renku.knowledgegraph.users.projects

import cats.MonadThrow
import cats.implicits._
import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax._
import io.renku.config.renku
import io.renku.data.Message
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model._
import io.renku.knowledgegraph.docs
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

import java.time.Instant

object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[docs.EndpointDocs] = for {
    renkuUrl <- RenkuUrlLoader[F]()
    apiUrl   <- renku.ApiUrl[F]()
  } yield new EndpointDocsImpl()(renkuUrl, apiUrl)
}

private class EndpointDocsImpl()(implicit renkuUrl: RenkuUrl, renkuApiUrl: renku.ApiUrl) extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    GET(
      "User Projects",
      "Finds Projects the user owns or maintains",
      Uri / "users" / userId / "projects" :? state,
      Status.Ok -> Response("Found projects",
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

  private lazy val userId = Parameter.Path(
    "userId",
    Schema.Integer,
    "User's GitLab identifier".some
  )
  private lazy val state = Parameter.Query(
    "state",
    Schema.String,
    "to filter by project state; allowed values: 'ACTIVATED', 'NOT_ACTIVATED', 'ALL'; default value is 'ALL'".some,
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

  private lazy val example = Json.arr(
    model.Project
      .Activated(
        projects.Name("name"),
        projects.Slug("group/subgroup/name"),
        projects.Visibility.Public,
        projects.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
        persons.Name("Jan Kowalski").some,
        List(projects.Keyword("key")),
        projects.Description("Some project").some
      )
      .asJson,
    model.Project
      .NotActivated(
        projects.GitLabId(1),
        projects.Name("name"),
        projects.Slug("group/subgroup/name"),
        projects.Visibility.Public,
        projects.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
        persons.GitLabId(1).some,
        persons.Name("Jan Kowalski").some,
        List(projects.Keyword("key")),
        projects.Description("Some project").some
      )
      .asJson
  )
}

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

package io.renku.knowledgegraph.projects.details

import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.data.Message.Codecs._
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.versions.SchemaVersion
import io.renku.graph.model.{GitLabUrl, persons, projects}
import io.renku.knowledgegraph.docs
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._
import model.Forking.ForksCount
import model.Permissions.{AccessLevel, GroupAccessLevel}
import model.Project.StarsCount
import model.Statistics.{CommitsCount, JobArtifactsSize, LsfObjectsSize, RepositorySize, StorageSize}
import model.Urls.{HttpUrl, ReadmeUrl, SshUrl, WebUrl}
import model._

import java.time.Instant

object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[docs.EndpointDocs] =
    GitLabUrlLoader[F]().flatMap(gitLabUrl =>
      ProjectJsonEncoder[F].map(new EndpointDocsImpl(_, ProjectJsonLDEncoder)(gitLabUrl))
    )
}

private class EndpointDocsImpl(projectJsonEncoder: ProjectJsonEncoder, projectJsonLDEncoder: ProjectJsonLDEncoder)(
    implicit gitLabUrl: GitLabUrl
) extends docs.EndpointDocs {

  override lazy val path: Path = Path(
    GET(
      "Project Details",
      "Finds Project details",
      Uri / "projects" / namespace / projectName,
      Status.Ok -> Response(
        "Details found",
        Contents(
          MediaType.`application/json`("Sample data", projectJsonEncoder encode example),
          MediaType.`application/ld+json`("Sample data", projectJsonLDEncoder encode example)
        )
      ),
      Status.Unauthorized -> Response(
        "Unauthorized",
        Contents(MediaType.`application/json`("Invalid token", Message.Info("Unauthorized")))
      ),
      Status.NotFound -> Response(
        "Details not found or no privileges",
        Contents(
          MediaType.`application/json`("Reason", Message.Info("No project namespace/project found"))
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

  private val example = Project(
    projects.ResourceId("http://renkulab.io/projects/namespace/name"),
    projects.GitLabId(123),
    projects.Path("namespace/name"),
    projects.Name("name"),
    projects.Description("description").some,
    projects.Visibility.Public,
    Creation(
      projects.DateCreated(Instant.parse("2012-11-15T10:00:00.000Z")),
      Creator(persons.ResourceId("http://renkulab.io/persons/2"),
              persons.Name("John"),
              persons.Email("john@mail.com").some,
              persons.Affiliation("SDSC").some
      ).some
    ),
    projects.DateModified(Instant.parse("2012-11-16T10:00:00.000Z")),
    Urls(
      SshUrl("git@github.com:namespace/name.git"),
      HttpUrl("https://github.com/namespace/name.git"),
      WebUrl("https://github.com/namespace/name"),
      ReadmeUrl("https://github.com/namespace/name/README.md").some
    ),
    Forking(
      ForksCount(1),
      ParentProject(
        projects.ResourceId("http://renkulab.io/projects/namespace/fork"),
        projects.Path("namespace/fork"),
        projects.Name("fork"),
        Creation(
          projects.DateCreated(Instant.parse("2012-11-17T10:00:00.000Z")),
          Creator(persons.ResourceId("http://renkulab.io/persons/3"),
                  persons.Name("Goeff"),
                  persons.Email("goeff@mail.com").some,
                  persons.Affiliation("SDSC").some
          ).some
        )
      ).some
    ),
    Set(projects.Keyword("key")),
    StarsCount(1),
    Permissions(GroupAccessLevel(AccessLevel.Owner)),
    Statistics(CommitsCount(1), StorageSize(1024), RepositorySize(1024), LsfObjectsSize(1024), JobArtifactsSize(0)),
    SchemaVersion("9").some,
    List(ImageUri("image.png"))
  )
}

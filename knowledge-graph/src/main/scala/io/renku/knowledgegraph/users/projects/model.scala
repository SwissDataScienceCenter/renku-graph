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

package io.renku.knowledgegraph.users.projects

import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.config.renku
import io.renku.graph.model.{RenkuUrl, persons, projects}
import io.renku.http.rest.Links.{Href, Link, Method, Rel, _links}
import io.renku.knowledgegraph.projectdetails

private object model {

  sealed trait Project extends Product with Serializable {
    val name:         projects.Name
    val path:         projects.Path
    val visibility:   projects.Visibility
    val dateCreated:  projects.DateCreated
    val maybeCreator: Option[persons.Name]
    val keywords:     List[projects.Keyword]
    val maybeDesc:    Option[projects.Description]
  }

  object Project {

    final case class Activated(
        name:         projects.Name,
        path:         projects.Path,
        visibility:   projects.Visibility,
        dateCreated:  projects.DateCreated,
        maybeCreator: Option[persons.Name],
        keywords:     List[projects.Keyword],
        maybeDesc:    Option[projects.Description]
    ) extends Project

    object Activated {
      implicit def encoder(implicit renkuApiUrl: renku.ApiUrl): Encoder[Activated] =
        Encoder.instance[Activated] { project =>
          json"""{
            "path":        ${project.path.value},
            "name":        ${project.name.value},
            "visibility":  ${project.visibility.value},
            "date":        ${project.dateCreated.value},
            "creator":     ${project.maybeCreator.map(_.value)},
            "keywords":    ${project.keywords.sorted.map(_.value)},
            "description": ${project.maybeDesc.map(_.value)}
          }""" deepMerge _links(
            Link(Rel("details") -> projectdetails.Endpoint.href(renkuApiUrl, project.path))
          )
        }
    }

    final case class NotActivated(
        id:           projects.Id,
        name:         projects.Name,
        path:         projects.Path,
        visibility:   projects.Visibility,
        dateCreated:  projects.DateCreated,
        maybeCreator: Option[persons.Name],
        keywords:     List[projects.Keyword],
        maybeDesc:    Option[projects.Description]
    ) extends Project

    object NotActivated {
      implicit def encoder(implicit renkuUrl: RenkuUrl): Encoder[NotActivated] =
        Encoder.instance[NotActivated] { project =>
          json"""{
            "id":          ${project.id.value},
            "path":        ${project.path.value},
            "name":        ${project.name.value},
            "visibility":  ${project.visibility.value},
            "date":        ${project.dateCreated.value},
            "creator":     ${project.maybeCreator.map(_.value)},
            "keywords":    ${project.keywords.sorted.map(_.value)},
            "description": ${project.maybeDesc.map(_.value)}
          }""" deepMerge _links(
            Link(Rel("activation"),
                 Href(renkuUrl / "api" / "projects" / project.id.show / "graph" / "webhooks"),
                 Method.POST
            )
          )
        }
    }

    implicit def encoder(implicit renkuUrl: RenkuUrl, renkuApiUrl: renku.ApiUrl): Encoder[Project] = Encoder.instance {
      case p: Project.Activated    => p.asJson
      case p: Project.NotActivated => p.asJson
    }
  }
}

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

package io.renku.triplesgenerator.api

import cats.syntax.all._
import cats.{Foldable, Show}
import io.circe.literal._
import io.circe.{Decoder, Encoder}
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.{persons, projects}
import io.renku.tinytypes.json.TinyTypeDecoders._

final case class NewProject(
    name:             projects.Name,
    slug:             projects.Slug,
    maybeDescription: Option[projects.Description],
    dateCreated:      projects.DateCreated,
    creator:          NewProject.Creator,
    keywords:         Set[projects.Keyword],
    visibility:       projects.Visibility,
    images:           List[ImageUri]
)

object NewProject {

  final case class Creator(name: persons.Name, id: persons.GitLabId) {
    val role: projects.Role = projects.Role.Owner
  }

  object Creator {
    implicit val show: Show[Creator] = Show.show { case Creator(name, id) =>
      s"name=$name, id=$id"
    }
  }

  implicit val encoder: Encoder[NewProject] = Encoder.instance {
    case NewProject(name, slug, maybeDescription, dateCreated, creator, keywords, visibility, images) =>
      json"""{
        "name":        $name,
        "slug":        $slug,
        "description": $maybeDescription,
        "dateCreated": $dateCreated,
        "keywords":    $keywords,
        "visibility":  $visibility,
        "images":      $images,
        "creator": {
          "name": ${creator.name},
          "id":   ${creator.id}
        }
      }""".dropNullValues
  }

  implicit val decoder: Decoder[NewProject] = Decoder.instance { cur =>
    for {
      name        <- cur.downField("name").as[projects.Name]
      slug        <- cur.downField("slug").as[projects.Slug]
      maybeDesc   <- cur.downField("description").as[Option[projects.Description]]
      dateCreated <- cur.downField("dateCreated").as[projects.DateCreated]
      keywords    <- cur.downField("keywords").as[List[projects.Keyword]].map(_.toSet)
      visibility  <- cur.downField("visibility").as[projects.Visibility]
      images      <- cur.downField("images").as[List[ImageUri]]
      creatorName <- cur.downField("creator").downField("name").as[persons.Name]
      creatorId   <- cur.downField("creator").downField("id").as[persons.GitLabId]
    } yield NewProject(name,
                       slug,
                       maybeDesc,
                       dateCreated,
                       Creator(creatorName, creatorId),
                       keywords,
                       visibility,
                       images
    )
  }

  implicit val show: Show[NewProject] = Show.show {
    case NewProject(name, slug, maybeDescription, dateCreated, creator, keywords, visibility, images) =>
      def showIterable[M[_]: Foldable, T](iterable: M[T])(implicit show: Show[T]) =
        iterable match {
          case it if it.isEmpty => None
          case it               => it.mkString_("[", ", ", "]").some
        }

      List(
        s"name=$name".some,
        s"slug=$slug".some,
        maybeDescription.map(v => s"description=$v"),
        s"dateCreated=$dateCreated".some,
        show"creator=($creator)".some,
        showIterable(keywords.toList).map(v => s"keywords=$v"),
        showIterable(images).map(v => s"images=$v"),
        s"visibility=$visibility".some
      ).flatten.mkString(", ")
  }
}

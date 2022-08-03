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

package io.renku.knowledgegraph.datasets

import Dataset.DatasetCreator
import DatasetSearchResult.ProjectsCount
import cats.syntax.all._
import io.circe.literal._
import io.circe.{Encoder, Json}
import io.renku.config
import io.renku.graph.model.datasets.{Date, DatePublished, Description, Identifier, ImageUri, Keyword, Name, Title}
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.rest.Links.{Href, Link, Rel, _links}
import io.renku.json.JsonOps._
import io.renku.tinytypes.constraints.NonNegativeInt
import io.renku.tinytypes.json.TinyTypeEncoders._
import io.renku.tinytypes.{IntTinyType, TinyTypeFactory}

final case class DatasetSearchResult(
    id:                  Identifier,
    title:               Title,
    name:                Name,
    maybeDescription:    Option[Description],
    creators:            List[DatasetCreator],
    date:                Date,
    exemplarProjectPath: projects.Path,
    projectsCount:       ProjectsCount,
    keywords:            List[Keyword],
    images:              List[ImageUri]
)

object DatasetSearchResult {

  implicit def encoder(implicit
      gitLabUrl:   GitLabUrl,
      renkuApiUrl: config.renku.ApiUrl
  ): Encoder[DatasetSearchResult] = Encoder.instance[DatasetSearchResult] {
    case DatasetSearchResult(id,
                             title,
                             name,
                             maybeDescription,
                             creators,
                             date,
                             exemplarProjectPath,
                             projectsCount,
                             keywords,
                             images
        ) =>
      json"""{
        "identifier": $id,
        "title": $title,
        "name": $name,
        "published": ${creators -> date},
        "date": ${date.instant},
        "projectsCount": $projectsCount,
        "keywords": $keywords,
        "images": ${images -> exemplarProjectPath}
      }"""
        .addIfDefined("description" -> maybeDescription)
        .deepMerge(_links(Link(Rel("details") -> DatasetEndpoint.href(renkuApiUrl, id))))
  }

  private implicit lazy val publishingEncoder: Encoder[(List[DatasetCreator], Date)] = Encoder.instance {
    case (creators, DatePublished(date)) => json"""{
    "creator": $creators,
    "datePublished": $date
  }"""
    case (creators, _) => json"""{
    "creator": $creators
  }"""
  }

  private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] {
    case DatasetCreator(maybeEmail, name, _) => json"""{
    "name": $name
  }""" addIfDefined ("email" -> maybeEmail)
  }

  private implicit def imagesEncoder(implicit gitLabUrl: GitLabUrl): Encoder[(List[ImageUri], projects.Path)] =
    Encoder.instance[(List[ImageUri], projects.Path)] { case (imageUris, exemplarProjectPath) =>
      Json.arr(imageUris.map {
        case uri: ImageUri.Relative =>
          json"""{
          "location": $uri  
        }""" deepMerge _links(
            Link(Rel("view") -> Href(gitLabUrl / exemplarProjectPath / "raw" / "master" / uri))
          )
        case uri: ImageUri.Absolute =>
          json"""{
          "location": $uri  
        }""" deepMerge _links(
            Link(Rel("view") -> Href(uri.show))
          )
      }: _*)
    }

  final class ProjectsCount private (val value: Int) extends AnyVal with IntTinyType

  implicit object ProjectsCount
      extends TinyTypeFactory[ProjectsCount](new ProjectsCount(_))
      with NonNegativeInt[ProjectsCount]
}

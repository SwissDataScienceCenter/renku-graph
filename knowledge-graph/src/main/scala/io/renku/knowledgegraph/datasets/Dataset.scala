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

import Dataset._
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.graph.model.datasets.{Date, DateCreated, DatePublished, DerivedFrom, Description, Identifier, ImageUri, Keyword, Name, OriginalIdentifier, PartLocation, ResourceId, SameAs, Title}
import io.renku.graph.model.persons.{Affiliation, Email}
import io.renku.graph.model.projects.Path
import io.renku.graph.model.{GitLabUrl, persons, projects}
import io.renku.http.rest.Links.{Href, Link, Rel, _links}
import io.renku.knowledgegraph.projects.rest.ProjectEndpoint
import io.renku.tinytypes.json.TinyTypeEncoders._

sealed trait Dataset extends Product with Serializable {
  val resourceId:       ResourceId
  val id:               Identifier
  val title:            Title
  val name:             Name
  val maybeDescription: Option[Description]
  val creators:         List[DatasetCreator]
  val date:             Date
  val parts:            List[DatasetPart]
  val project:          DatasetProject
  val usedIn:           List[DatasetProject]
  val keywords:         List[Keyword]
  val versions:         DatasetVersions
  val images:           List[ImageUri]
}

object Dataset {

  final case class NonModifiedDataset(resourceId:       ResourceId,
                                      id:               Identifier,
                                      title:            Title,
                                      name:             Name,
                                      sameAs:           SameAs,
                                      versions:         DatasetVersions,
                                      maybeDescription: Option[Description],
                                      creators:         List[DatasetCreator],
                                      date:             Date,
                                      parts:            List[DatasetPart],
                                      project:          DatasetProject,
                                      usedIn:           List[DatasetProject],
                                      keywords:         List[Keyword],
                                      images:           List[ImageUri]
  ) extends Dataset

  final case class ModifiedDataset(resourceId:       ResourceId,
                                   id:               Identifier,
                                   title:            Title,
                                   name:             Name,
                                   derivedFrom:      DerivedFrom,
                                   versions:         DatasetVersions,
                                   maybeDescription: Option[Description],
                                   creators:         List[DatasetCreator],
                                   date:             DateCreated,
                                   parts:            List[DatasetPart],
                                   project:          DatasetProject,
                                   usedIn:           List[DatasetProject],
                                   keywords:         List[Keyword],
                                   images:           List[ImageUri]
  ) extends Dataset

  final case class DatasetCreator(maybeEmail: Option[Email], name: persons.Name, maybeAffiliation: Option[Affiliation])

  final case class DatasetPart(location: PartLocation)
  final case class DatasetVersions(initial: OriginalIdentifier)

  final case class DatasetProject(path: Path, name: projects.Name)

  // format: off
  implicit def encoder(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl): Encoder[Dataset] = Encoder.instance[Dataset] { dataset =>
    Json.obj(
      List(
        ("identifier" -> dataset.id.asJson).some,
        ("name" -> dataset.name.asJson).some,
        ("title" -> dataset.title.asJson).some,
        ("url" -> dataset.resourceId.asJson).some,
        dataset match {
          case ds: NonModifiedDataset => ("sameAs" -> ds.sameAs.asJson).some
          case ds: ModifiedDataset    => ("derivedFrom" -> ds.derivedFrom.asJson).some
        },
        ("versions" -> dataset.versions.asJson).some,
        dataset.maybeDescription.map(description => "description" -> description.asJson),
        ("published" -> (dataset.creators -> dataset.date).asJson).some,
        dataset.date match {
          case DatePublished(_)  => Option.empty[(String, Json)]
          case DateCreated(date) => ("created" -> date.asJson).some
        },
        ("hasPart" -> dataset.parts.asJson).some,
        ("project" -> dataset.project.asJson).some,
        ("usedIn" -> dataset.usedIn.asJson).some,
        ("keywords" -> dataset.keywords.asJson).some,
        ("images" -> (dataset.images, dataset.project).asJson).some
      ).flatten: _*
    ) deepMerge _links(
      Rel.Self -> DatasetEndpoint.href(renkuApiUrl, dataset.id),
      Rel("initial-version") -> DatasetEndpoint.href(renkuApiUrl, dataset.versions.initial.value)
    )
  }
  // format: on

  private implicit lazy val publishingEncoder: Encoder[(List[DatasetCreator], Date)] = Encoder.instance {
    case (creators, DatePublished(date)) => json"""{
      "datePublished": $date,
      "creator"      : $creators
    }"""
    case (creators, _) => json"""{
      "creator": $creators
    }"""
  }

  // format: off
  private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] { creator =>
    Json.obj(List(
      ("name" -> creator.name.asJson).some,
      creator.maybeEmail.map(email => "email" -> email.asJson),
      creator.maybeAffiliation.map(affiliation => "affiliation" -> affiliation.asJson)
    ).flatten: _*)
  }
  // format: on

  private implicit lazy val partEncoder: Encoder[DatasetPart] = Encoder.instance[DatasetPart] { part =>
    json"""{
      "atLocation": ${part.location}
    }"""
  }

  private implicit def projectEncoder(implicit renkuApiUrl: renku.ApiUrl): Encoder[DatasetProject] =
    Encoder.instance[DatasetProject] { project =>
      json"""{
      "path": ${project.path},
      "name": ${project.name}
    }""" deepMerge _links(Link(Rel("project-details") -> ProjectEndpoint.href(renkuApiUrl, project.path)))
    }

  private implicit lazy val versionsEncoder: Encoder[DatasetVersions] = Encoder.instance[DatasetVersions] { versions =>
    json"""{
      "initial": ${versions.initial}
    }"""
  }

  private implicit def imagesEncoder(implicit gitLabUrl: GitLabUrl): Encoder[(List[ImageUri], DatasetProject)] =
    Encoder.instance[(List[ImageUri], DatasetProject)] { case (imageUris, project) =>
      Json.arr(imageUris.map {
        case uri: ImageUri.Relative =>
          json"""{
            "location": $uri
          }""" deepMerge _links(
            Link(Rel("view") -> Href(gitLabUrl / project.path / "raw" / "master" / uri))
          )
        case uri: ImageUri.Absolute =>
          json"""{
            "location": $uri
          }""" deepMerge _links(
            Link(Rel("view") -> Href(uri.show))
          )
      }: _*)
    }
}

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
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.graph.model
import io.renku.graph.model.datasets._
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{GitLabUrl, RenkuUrl}
import io.renku.http.rest.Links.{Href, Link, Rel, _links}
import io.renku.json.JsonOps._

private sealed trait Dataset extends Product with Serializable {
  val resourceId:         ResourceId
  val title:              Title
  val name:               Name
  val versions:           DatasetVersions
  val maybeInitialTag:    Option[Tag]
  val maybeDescription:   Option[Description]
  val creators:           List[DatasetCreator]
  val createdOrPublished: CreatedOrPublished
  val parts:              List[DatasetPart]
  val project:            DatasetProject
  val usedIn:             List[DatasetProject]
  val keywords:           List[Keyword]
  val images:             List[ImageUri]

  lazy val widen: Dataset = this
  def fold[A](fnm: Dataset.NonModifiedDataset => A, fm: Dataset.ModifiedDataset => A): A
}

private object Dataset {

  final case class NonModifiedDataset(resourceId:         ResourceId,
                                      title:              Title,
                                      name:               Name,
                                      sameAs:             SameAs,
                                      versions:           DatasetVersions,
                                      maybeInitialTag:    Option[Tag],
                                      maybeDescription:   Option[Description],
                                      creators:           List[DatasetCreator],
                                      createdOrPublished: CreatedOrPublished,
                                      parts:              List[DatasetPart],
                                      project:            DatasetProject,
                                      usedIn:             List[DatasetProject],
                                      keywords:           List[Keyword],
                                      images:             List[ImageUri]
  ) extends Dataset {
    def fold[A](fnm: Dataset.NonModifiedDataset => A, fm: Dataset.ModifiedDataset => A): A = fnm(this)
  }

  final case class ModifiedDataset(resourceId:         ResourceId,
                                   title:              Title,
                                   name:               Name,
                                   derivedFrom:        DerivedFrom,
                                   versions:           DatasetVersions,
                                   maybeInitialTag:    Option[Tag],
                                   maybeDescription:   Option[Description],
                                   creators:           List[DatasetCreator],
                                   createdOrPublished: CreatedOrPublished,
                                   dateModified:       DateModified,
                                   parts:              List[DatasetPart],
                                   project:            DatasetProject,
                                   usedIn:             List[DatasetProject],
                                   keywords:           List[Keyword],
                                   images:             List[ImageUri]
  ) extends Dataset {
    def fold[A](fnm: Dataset.NonModifiedDataset => A, fm: Dataset.ModifiedDataset => A): A = fm(this)
  }

  final case class DatasetPart(location: PartLocation)
  final case class DatasetVersions(initial: OriginalIdentifier)
  final case class Tag(name: model.publicationEvents.Name, maybeDesc: Option[model.publicationEvents.Description])

  final case class DatasetProject(id:                model.projects.ResourceId,
                                  path:              model.projects.Path,
                                  name:              model.projects.Name,
                                  visibility:        Visibility,
                                  datasetIdentifier: model.datasets.Identifier
  )

  // format: off
  def encoder(requestedDataset: RequestedDataset)(implicit gitLabUrl: GitLabUrl, renkuApiUrl: renku.ApiUrl, renkuUrl: RenkuUrl): Encoder[Dataset] =
    Encoder.instance[Dataset] { dataset =>
      Json.obj(
        List(
          ("identifier" -> requestedDataset.asJson).some,
          ("name" -> dataset.name.asJson).some,
          ("slug" -> dataset.name.asJson).some,
          ("title" -> dataset.title.asJson).some,
          ("url" -> dataset.resourceId.asJson).some,
          dataset match {
            case ds: NonModifiedDataset => ("sameAs" -> ds.sameAs.asJson).some
            case ds: ModifiedDataset    => ("derivedFrom" -> ds.derivedFrom.asJson).some
          },
          ("versions" -> dataset.versions.asJson).some,
          dataset.maybeInitialTag.map(tag => "tags" -> json"""{"initial": ${tag.asJson}}"""),
          dataset.maybeDescription.map(description => "description" -> description.asJson),
          ("published" -> (dataset.creators -> dataset.createdOrPublished).asJson).some,
          dataset.createdOrPublished match {
            case DatePublished(_)  => Option.empty[(String, Json)]
            case DateCreated(date) => ("created" -> date.asJson).some
          },
          dataset match {
            case m: Dataset.ModifiedDataset => ("dateModified" -> m.dateModified.asJson).some
            case _ => None
          },
          ("hasPart" -> dataset.parts.asJson).some,
          ("project" -> dataset.project.asJson).some,
          ("usedIn" -> dataset.usedIn.asJson).some,
          ("keywords" -> dataset.keywords.asJson).some,
          ("images" -> (dataset.images, dataset.project).asJson).some
        ).flatten: _*
      ) deepMerge _links(
        Rel.Self -> Endpoint.href(renkuApiUrl, requestedDataset),
        Rel("initial-version") -> Endpoint.href(renkuApiUrl, RequestedDataset(dataset.versions.initial.asIdentifier)),
        Rel("tags") -> projects.datasets.tags.Endpoint.href(renkuApiUrl, dataset.project.path, dataset.name),
      )
  }
  // format: on

  private implicit lazy val publishingEncoder: Encoder[(List[DatasetCreator], CreatedOrPublished)] = Encoder.instance {
    case (creators, DatePublished(date)) => json"""{
      "datePublished": $date,
      "creator"      : $creators
    }"""
    case (creators, _) => json"""{
      "creator": $creators
    }"""
  }

  private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] { creator =>
    Json.obj(
      List(
        ("name" -> creator.name.asJson).some,
        creator.maybeEmail.map(email => "email" -> email.asJson),
        creator.maybeAffiliation.map(affiliation => "affiliation" -> affiliation.asJson)
      ).flatten: _*
    )
  }

  private implicit lazy val partEncoder: Encoder[DatasetPart] = Encoder.instance[DatasetPart] { part =>
    json"""{
      "atLocation": ${part.location}
    }"""
  }

  private implicit def projectEncoder(implicit renkuApiUrl: renku.ApiUrl): Encoder[DatasetProject] =
    Encoder.instance[DatasetProject] { project =>
      json"""{
        "path":       ${project.path},
        "name":       ${project.name},
        "visibility": ${project.visibility},
        "dataset": {
          "identifier": ${project.datasetIdentifier}
        }
      }""" deepMerge _links(Link(Rel("project-details") -> projects.details.Endpoint.href(renkuApiUrl, project.path)))
    }

  private implicit lazy val versionsEncoder: Encoder[DatasetVersions] = Encoder.instance[DatasetVersions] { versions =>
    json"""{
      "initial": ${versions.initial}
    }"""
  }

  private implicit lazy val tagEncoder: Encoder[Tag] = Encoder.instance[Tag] { tag =>
    json"""{
      "name": ${tag.name}
    }""" addIfDefined ("description" -> tag.maybeDesc)
  }

  private implicit def imagesEncoder(implicit gitLabUrl: GitLabUrl): Encoder[(List[ImageUri], DatasetProject)] =
    Encoder.instance[(List[ImageUri], DatasetProject)] { case (imageUris, project) =>
      Json.arr(imageUris.map {
        case uri: ImageUri.Relative =>
          json"""{
            "location": $uri
          }""" deepMerge _links(Link(Rel("view") -> Href(gitLabUrl / project.path / "raw" / "master" / uri)))
        case uri: ImageUri.Absolute =>
          json"""{
            "location": $uri
          }""" deepMerge _links(Link(Rel("view") -> Href(uri.show)))
      }: _*)
    }
}

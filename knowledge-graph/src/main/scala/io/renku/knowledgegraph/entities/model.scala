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

package io.renku.knowledgegraph.entities

import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, Json}
import io.renku.config.renku
import io.renku.graph.model._
import io.renku.http.rest.Links.{Href, Link, Rel, _links}
import io.renku.json.JsonOps._
import io.renku.knowledgegraph.datasets.DatasetEndpoint
import io.renku.knowledgegraph.entities.Endpoint.Criteria
import io.renku.knowledgegraph.projects.rest.ProjectEndpoint
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.FiniteFloat
import io.renku.tinytypes.json.TinyTypeEncoders._
import io.renku.tinytypes.json.{TinyTypeDecoders, TinyTypeEncoders}

import java.time.Instant

object model {

  sealed trait Entity extends Product with Serializable {
    type Name <: StringTinyType
    val name: Name

    type Date <: TinyType
    val date: Date

    val matchingScore: MatchingScore
  }

  object Entity {

    final case class Project(
        matchingScore:    MatchingScore,
        path:             projects.Path,
        name:             projects.Name,
        visibility:       projects.Visibility,
        date:             projects.DateCreated,
        maybeCreator:     Option[persons.Name],
        keywords:         List[projects.Keyword],
        maybeDescription: Option[projects.Description]
    ) extends Entity {
      override type Name = projects.Name
      override type Date = projects.DateCreated
    }

    object Project {
      private[entities] implicit def encoder(implicit renkuApiUrl: renku.ApiUrl): Encoder[model.Entity.Project] =
        Encoder.instance { project =>
          json"""{
            "type":          ${Criteria.Filters.EntityType.Project.value},
            "matchingScore": ${project.matchingScore},
            "name":          ${project.name},
            "path":          ${project.path},
            "namespace":     ${project.path.toNamespaces.mkString("/")},
            "visibility":    ${project.visibility},
            "date":          ${project.date},
            "keywords":      ${project.keywords}
          }"""
            .addIfDefined("creator" -> project.maybeCreator)
            .addIfDefined("description" -> project.maybeDescription)
            .deepMerge(
              _links(
                Link(Rel("details") -> ProjectEndpoint.href(renkuApiUrl, project.path))
              )
            )
        }
    }

    final case class Dataset(
        matchingScore:       MatchingScore,
        identifier:          datasets.Identifier,
        name:                datasets.Name,
        visibility:          projects.Visibility,
        date:                datasets.Date,
        creators:            List[persons.Name],
        keywords:            List[datasets.Keyword],
        maybeDescription:    Option[datasets.Description],
        images:              List[datasets.ImageUri],
        exemplarProjectPath: projects.Path
    ) extends Entity {
      override type Name = datasets.Name
      override type Date = datasets.Date
    }

    object Dataset {
      private[entities] implicit def encoder(implicit
          gitLabUrl:   GitLabUrl,
          renkuApiUrl: renku.ApiUrl
      ): Encoder[model.Entity.Dataset] = {

        implicit lazy val imagesEncoder: Encoder[(List[datasets.ImageUri], projects.Path)] =
          Encoder.instance[(List[datasets.ImageUri], projects.Path)] { case (imageUris, exemplarProjectPath) =>
            Json.arr(imageUris.map {
              case uri: datasets.ImageUri.Relative =>
                json"""{
                  "location": $uri
                }""" deepMerge _links(
                  Link(Rel("view") -> Href(gitLabUrl / exemplarProjectPath / "raw" / "master" / uri))
                )
              case uri: datasets.ImageUri.Absolute =>
                json"""{
                  "location": $uri
                }""" deepMerge _links(Link(Rel("view") -> Href(uri.show)))
            }: _*)
          }

        Encoder.instance { ds =>
          json"""{
            "type":          ${Criteria.Filters.EntityType.Dataset.value},
            "matchingScore": ${ds.matchingScore},
            "name":          ${ds.name},
            "visibility":    ${ds.visibility},
            "date":          ${ds.date},
            "creators":      ${ds.creators},
            "keywords":      ${ds.keywords},
            "images":        ${(ds.images -> ds.exemplarProjectPath).asJson}
          }"""
            .addIfDefined("description" -> ds.maybeDescription)
            .deepMerge(
              _links(
                Link(Rel("details") -> DatasetEndpoint.href(renkuApiUrl, ds.identifier))
              )
            )
        }
      }
    }

    final case class Workflow(
        matchingScore:    MatchingScore,
        name:             plans.Name,
        visibility:       projects.Visibility,
        date:             plans.DateCreated,
        keywords:         List[plans.Keyword],
        maybeDescription: Option[plans.Description]
    ) extends Entity {
      override type Name = plans.Name
      override type Date = plans.DateCreated
    }

    object Workflow {
      private[entities] implicit lazy val encoder: Encoder[model.Entity.Workflow] =
        Encoder.instance { workflow =>
          json"""{
            "type":          ${Criteria.Filters.EntityType.Workflow.value},
            "matchingScore": ${workflow.matchingScore},
            "name":          ${workflow.name},
            "visibility":    ${workflow.visibility},
            "date":          ${workflow.date},
            "keywords":      ${workflow.keywords}
          }"""
            .addIfDefined("description" -> workflow.maybeDescription)
        }
    }

    final case class Person(
        matchingScore: MatchingScore,
        name:          persons.Name
    ) extends Entity {
      override type Name = persons.Name
      override type Date = Person.DateCreationFiller
      override val date: Person.DateCreationFiller = Person.DateCreationFiller
    }

    object Person {
      final case object DateCreationFiller extends InstantTinyType {
        override val value: Instant = Instant.EPOCH
      }
      type DateCreationFiller = DateCreationFiller.type

      private[entities] implicit lazy val encoder: Encoder[model.Entity.Person] =
        Encoder.instance { person =>
          json"""{
            "type":          ${Criteria.Filters.EntityType.Person.value},
            "matchingScore": ${person.matchingScore},
            "name":          ${person.name}
          }"""
        }
    }
  }

  final class MatchingScore private (val value: Float) extends AnyVal with FloatTinyType
  object MatchingScore extends TinyTypeFactory[MatchingScore](new MatchingScore(_)) with FiniteFloat[MatchingScore] {
    val min:                  MatchingScore          = MatchingScore(1.0f)
    implicit val jsonEncoder: Encoder[MatchingScore] = TinyTypeEncoders.floatEncoder
    implicit val jsonDecoder: Decoder[MatchingScore] = TinyTypeDecoders.floatDecoder(MatchingScore)
  }
}

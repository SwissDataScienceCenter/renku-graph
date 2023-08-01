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

package io.renku.entities.search

import cats.data.NonEmptyList
import io.circe.{Decoder, Encoder}
import io.renku.graph.model._
import io.renku.graph.model.images.ImageUri
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.FiniteFloat
import io.renku.tinytypes.json.TinyTypeDecoders

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
        slug:             projects.Slug,
        name:             projects.Name,
        visibility:       projects.Visibility,
        date:             projects.DateCreated,
        dateModified:     projects.DateModified,
        maybeCreator:     Option[persons.Name],
        keywords:         List[projects.Keyword],
        maybeDescription: Option[projects.Description],
        images:           List[ImageUri]
    ) extends Entity {
      override type Name = projects.Name
      override type Date = projects.DateCreated
    }

    final case class Dataset(
        matchingScore:       MatchingScore,
        sameAs:              datasets.TopmostSameAs,
        name:                datasets.Name,
        visibility:          projects.Visibility,
        date:                datasets.CreatedOrPublished,
        dateModified:        Option[datasets.DateModified],
        creators:            List[persons.Name],
        keywords:            List[datasets.Keyword],
        maybeDescription:    Option[datasets.Description],
        images:              List[ImageUri],
        exemplarProjectSlug: projects.Slug
    ) extends Entity {
      override type Name = datasets.Name
      override type Date = datasets.CreatedOrPublished
    }

    final case class Workflow(
        matchingScore:    MatchingScore,
        name:             plans.Name,
        visibility:       projects.Visibility,
        date:             plans.DateCreated,
        keywords:         List[plans.Keyword],
        maybeDescription: Option[plans.Description],
        workflowType:     Workflow.WorkflowType
    ) extends Entity {
      override type Name = plans.Name
      override type Date = plans.DateCreated
    }

    object Workflow {
      sealed trait WorkflowType { self: Product =>
        final def name: String = self.productPrefix.toLowerCase
      }
      object WorkflowType {
        case object Composite extends WorkflowType
        case object Step      extends WorkflowType

        val all: NonEmptyList[WorkflowType] =
          NonEmptyList.of(Composite, Step)

        def fromName(str: String): Either[String, WorkflowType] =
          all.find(_.name.equalsIgnoreCase(str)).toRight(s"Invalid workflowType name: $str")

        implicit val encoder: Encoder[WorkflowType] =
          Encoder.encodeString.contramap(_.name)
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
    }
  }

  final class MatchingScore private (val value: Float) extends AnyVal with FloatTinyType
  object MatchingScore extends TinyTypeFactory[MatchingScore](new MatchingScore(_)) with FiniteFloat[MatchingScore] {
    val min:                  MatchingScore          = MatchingScore(1.0f)
    implicit val jsonDecoder: Decoder[MatchingScore] = TinyTypeDecoders.floatDecoder(MatchingScore)
  }
}

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

import io.circe.{Decoder, Encoder}
import io.renku.graph.model.{datasets, persons, plans, projects}
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.FiniteFloat
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
    implicit val jsonEncoder: Encoder[MatchingScore] = TinyTypeEncoders.floatEncoder
    implicit val jsonDecoder: Decoder[MatchingScore] = TinyTypeDecoders.floatDecoder(MatchingScore)
  }
}

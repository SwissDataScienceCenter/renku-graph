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

package io.renku.cli.model

import cats.syntax.either._
import io.circe.DecodingFailure
import io.renku.cli.model.Ontologies.{Prov, Schema}
import io.renku.graph.model.persons.{Affiliation, Email, Name, ResourceId}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliPerson(
    resourceId:  ResourceId,
    name:        Name,
    email:       Option[Email],
    affiliation: Option[Affiliation]
) extends CliModel

object CliPerson {

  private val entityTypes: EntityTypes = EntityTypes.of(Schema.Person, Prov.Person)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  implicit def jsonLDDecoder: JsonLDDecoder[CliPerson] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        names            <- cursor.downField(Schema.name).as[List[Name]]
        maybeEmail       <- cursor.downField(Schema.email).as[Option[Email]]
        maybeAffiliation <- cursor.downField(Schema.affiliation).as[List[Affiliation]].map(_.reverse.headOption)
        name <- if (names.isEmpty) DecodingFailure(s"No name on Person $resourceId", Nil).asLeft
                else names.reverse.head.asRight
        person =
          CliPerson(resourceId, name, maybeEmail, maybeAffiliation)
      } yield person
    }

  implicit def jsonLDEncoder: JsonLDEncoder[CliPerson] =
    JsonLDEncoder.instance { person =>
      JsonLD.entity(
        person.resourceId.asEntityId,
        entityTypes,
        List(
          Some(Schema.name -> person.name.asJsonLD),
          person.email.map(m => Schema.email -> m.asJsonLD),
          person.affiliation.map(a => Schema.affiliation -> a.asJsonLD)
        ).flatten.toMap
      )
    }
}

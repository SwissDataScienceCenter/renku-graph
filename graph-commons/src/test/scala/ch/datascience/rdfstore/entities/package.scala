/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore

import cats.kernel.Semigroup
import ch.datascience.graph.Schemas
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets.SameAs
import ch.datascience.graph.model.projects.Visibility
import ch.datascience.graph.model.projects.Visibility.{Internal, Private, Public}
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._
import io.renku.jsonld._

import java.time.{Instant, LocalDate}

package object entities extends Schemas with EntitiesGenerators with ModelOps {

  implicit val fusekiBaseUrlToEntityId: FusekiBaseUrl => EntityId = url => EntityId of url.value
  implicit val renkuBaseUrlToEntityId:  RenkuBaseUrl => EntityId  = url => EntityId of url.value

  implicit lazy val visibilityEncoder: JsonLDEncoder[Visibility] = JsonLDEncoder.instance {
    case Private  => JsonLDEncoder.encodeString(Private.value)
    case Public   => JsonLDEncoder.encodeString(Public.value)
    case Internal => JsonLDEncoder.encodeString(Internal.value)
  }

  private implicit lazy val sameAsToPathSegment: SameAs => List[PathSegment] = sameAs => List(PathSegment(sameAs.value))

  implicit def stringTTEncoder[TT <: StringTinyType]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromString(v.value))
  implicit def relativePathTTEncoder[TT <: RelativePathTinyType]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromString(v.value))
  implicit def instantTTEncoder[TT <: TinyType { type V = Instant }]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromInstant(v.value))
  implicit def localDateTTEncoder[TT <: TinyType { type V = LocalDate }]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromLocalDate(v.value))
  implicit def booleanTTEncoder[TT <: BooleanTinyType]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromBoolean(v.value))
  implicit def intTTEncoder[TT <: IntTinyType]: JsonLDEncoder[TT] =
    JsonLDEncoder.instance(v => JsonLD.fromInt(v.value))

  implicit class EntityIdOps(entityId: EntityId) {
    lazy val asUrlEntityId: UrlfiedEntityId = UrlfiedEntityId(entityId.value.toString)
  }

  implicit val reverseSemigroup: Semigroup[Reverse] =
    (x: Reverse, y: Reverse) => Reverse(x.properties ++ y.properties)
}

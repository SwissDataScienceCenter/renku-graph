/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.jsonld.generators

import Generators._
import io.renku.jsonld._
import org.scalacheck.{Arbitrary, Gen}

object JsonLDGenerators {

  implicit val entityIds: Gen[EntityId] = httpUrls() map (EntityId.of(_))

  implicit val schemas: Gen[Schema] = for {
    baseUrl <- httpUrls()
    path    <- relativePaths(maxSegments = 3)
  } yield Schema.from(s"$baseUrl/$path")

  implicit val entityTypes: Gen[EntityType] = for {
    schema   <- schemas
    property <- nonBlankStrings()
  } yield EntityType.of(schema / property.value)

  implicit val entityTypesObject: Gen[EntityTypes] = nonEmptyList(entityTypes) map EntityTypes.apply

  private implicit val stringJsonLDs: Gen[JsonLD] = nonBlankStrings() map (_.value) map JsonLD.fromString
  private implicit val intJsonLDs:    Gen[JsonLD] = Arbitrary.arbInt.arbitrary map JsonLD.fromInt
  private implicit val longJsonLDs:   Gen[JsonLD] = Arbitrary.arbLong.arbitrary map JsonLD.fromLong
  implicit val jsonLDs: Gen[JsonLD] = Gen.oneOf(
    stringJsonLDs,
    intJsonLDs,
    longJsonLDs
  )

  implicit val properties: Gen[Property] = for {
    schema       <- schemas
    propertyName <- nonBlankStrings()
  } yield schema / propertyName.value

  implicit val entityProperties: Gen[(Property, JsonLD)] = for {
    property <- properties
    value    <- jsonLDs
  } yield property -> value
}

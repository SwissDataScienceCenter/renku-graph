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

import io.renku.jsonld.{EntityId, EntityType, Schema}
import org.scalacheck.Gen
import Generators._

object JsonLDGenerators {

  val absoluteUriEntityIds: Gen[EntityId] = httpUrls map EntityId.fromAbsoluteUri
  val relativeUriEntityIds: Gen[EntityId] = relativePaths() map EntityId.fromRelativeUri

  implicit val entityIds: Gen[EntityId] = Gen.oneOf(
    absoluteUriEntityIds,
    relativeUriEntityIds
  )

  implicit val schemas: Gen[Schema] = for {
    baseUrl <- httpUrls
    path    <- relativePaths(maxSegments = 3)
  } yield Schema.from(s"$baseUrl/$path")

  implicit val entityTypes: Gen[EntityType] = for {
    schema   <- schemas
    property <- nonBlankStrings()
  } yield EntityType.fromProperty(schema / property.value)
}

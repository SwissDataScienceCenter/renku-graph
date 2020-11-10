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

package io.renku.jsonld.flatten

import io.renku.jsonld.JsonLD
import io.renku.jsonld.JsonLD.{JsonLDArray, JsonLDEntity, MalformedJsonLD}
import cats.syntax.all._

trait JsonLDArrayFlatten extends Flatten {
  self: JsonLDArray =>
  override lazy val flatten: Either[MalformedJsonLD, List[JsonLD]] =
    for {
      flattenedJsons <- this.jsons
                          .foldLeft(Either.right[MalformedJsonLD, List[JsonLD]](List.empty[JsonLD])) {
                            case (acc, jsonLDEntity: JsonLDEntity) =>
                              for {
                                jsons    <- deNest(List(jsonLDEntity), List.empty[JsonLDEntity])
                                accRight <- acc
                              } yield accRight ++ jsons
                            case (acc, other) => acc.map(other +: _)
                          }
      flattenedArray <- checkForUniqueIds(flattenedJsons.distinct)
    } yield flattenedArray
}

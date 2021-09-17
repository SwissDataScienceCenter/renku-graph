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

package io.renku.jsonld.flatten

import io.renku.jsonld.JsonLD
import io.renku.jsonld.JsonLD.{JsonLDArray, JsonLDEntity, MalformedJsonLD}
import cats.syntax.all._

trait JsonLDArrayFlatten extends JsonLDFlatten {
  self: JsonLDArray =>
  import Flatten._
  import IDValidation._

  override lazy val flatten: Either[MalformedJsonLD, JsonLD] = for {
    flattened <- jsons.foldLeft(Either.right[MalformedJsonLD, List[JsonLD]](List.empty[JsonLD])) {
                   case (flattened, jsonLDEntity: JsonLDEntity) =>
                     flattened >>= (deNest(List(jsonLDEntity), _))
                   case (flattened, JsonLDArray(jsons)) =>
                     flattened >>= (deNest(jsons.toList, _))
                   case (flattened, other) => flattened.map(_ ::: other :: Nil)
                 }
    validated <- checkForUniqueIds(flattened.distinct)
    merged    <- JsonLD.arr(validated: _*).merge
  } yield merged
}

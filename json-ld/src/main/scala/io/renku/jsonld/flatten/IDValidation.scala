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
import io.renku.jsonld.JsonLD.{JsonLDEntity, MalformedJsonLD}

trait IDValidation {
  protected[flatten] def checkForUniqueIds(flattenedJsons: List[JsonLD]): Either[MalformedJsonLD, JsonLD] = if (
    areIdsUnique(flattenedJsons)
  )
    Right(JsonLD.arr(flattenedJsons: _*))
  else
    Left(MalformedJsonLD("Some entities share an ID even though they're not the same"))

  private def areIdsUnique(jsons: List[JsonLD]): Boolean =
    jsons
      .collect { case entity: JsonLDEntity => entity }
      .groupBy(entity => entity.id)
      .forall { case (_, entitiesPerId) =>
        entitiesPerId.forall(_ == entitiesPerId.head)
      }
}

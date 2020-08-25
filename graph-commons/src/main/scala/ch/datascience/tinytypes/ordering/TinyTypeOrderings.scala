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

package ch.datascience.tinytypes.ordering

import java.time.Instant

import ch.datascience.tinytypes.TinyType

object TinyTypeOrderings {

  implicit class TinyTypeOps[V](tinyType: TinyType { type V })(implicit val valueOrdering: Ordering[V]) {
    def compareTo(other: TinyType { type V }): Int =
      valueOrdering.compare(tinyType.value.asInstanceOf[V], other.value.asInstanceOf[V])
  }

  implicit val instantOrdering: Ordering[Instant] = (x: Instant, y: Instant) => x compareTo y
}

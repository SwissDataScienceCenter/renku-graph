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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import ch.datascience.rdfstore.{JsonLDTriples, SparqlQuery}
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.Update

final case class CuratedTriples(triples: JsonLDTriples, updates: List[Update])

object CuratedTriples {
  final case class Update(name: String, query: SparqlQuery)

  implicit class CuratedTriplesOps(curatedTriples: CuratedTriples) {
    def add(updates: Seq[Update]): CuratedTriples = curatedTriples.copy(
      updates = curatedTriples.updates ++ updates
    )
  }
}

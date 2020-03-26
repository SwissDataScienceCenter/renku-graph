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

import ch.datascience.generators.CommonGraphGenerators.jsonLDTriples
import ch.datascience.generators.Generators._
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.Update
import eu.timepit.refined.auto._
import org.scalacheck.Gen

object CurationGenerators {

  implicit lazy val curatedTriplesObjects: Gen[CuratedTriples] = curatedTriplesObjects(
    nonEmptyList(curationUpdates).map(_.toList)
  )

  def curatedTriplesObjects(updatesGenerator: Gen[List[Update]]): Gen[CuratedTriples] =
    for {
      triples <- jsonLDTriples
      updates <- updatesGenerator
    } yield CuratedTriples(triples, updates)

  implicit lazy val curationUpdates: Gen[Update] = for {
    name    <- nonBlankStrings(minLength = 5)
    message <- sentences() map (v => SparqlQuery("curation update", Set.empty, v.value))
  } yield Update(name, message)
}

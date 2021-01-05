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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import cats.MonadError
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators._
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.CurationUpdatesGroup
import eu.timepit.refined.auto._
import io.renku.jsonld.JsonLD
import org.scalacheck.Gen

object CurationGenerators {

  implicit def curatedTriplesObjects[Interpretation[_]](implicit
      ME: MonadError[Interpretation, Throwable]
  ): Gen[CuratedTriples[Interpretation]] =
    curatedTriplesObjects[Interpretation](
      nonEmptyList(curationUpdatesGroups[Interpretation]).map(_.toList)
    )

  def curatedTriplesObjects[Interpretation[_]](
      updatesGenerator: Gen[List[CurationUpdatesGroup[Interpretation]]]
  )(implicit ME:        MonadError[Interpretation, Throwable]): Gen[CuratedTriples[Interpretation]] =
    for {
      triples <- jsonLDTriples
      updates <- updatesGenerator
    } yield CuratedTriples[Interpretation](triples, updates)

  def curatedTriplesObjects[Interpretation[_]](
      triples:   JsonLD
  )(implicit ME: MonadError[Interpretation, Throwable]): Gen[CuratedTriples[Interpretation]] =
    for {
      updates <- nonEmptyList(curationUpdatesGroups[Interpretation])
    } yield CuratedTriples(JsonLDTriples(triples.flatten.fold(throw _, identity).toJson), updates.toList)

  implicit def curationUpdatesGroups[Interpretation[_]](implicit
      ME: MonadError[Interpretation, Throwable]
  ): Gen[CurationUpdatesGroup[Interpretation]] =
    for {
      name        <- nonBlankStrings(minLength = 5)
      sparqlQuery <- sparqlQueries
    } yield CurationUpdatesGroup[Interpretation](name, sparqlQuery)
}

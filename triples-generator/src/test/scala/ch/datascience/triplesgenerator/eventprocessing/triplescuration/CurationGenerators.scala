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

import cats.MonadError
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.jsonLDTriples
import ch.datascience.generators.Generators._
import ch.datascience.rdfstore.{JsonLDTriples, SparqlQuery}
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.UpdateFunction
import eu.timepit.refined.auto._
import io.renku.jsonld.JsonLD
import org.scalacheck.Gen

import scala.language.higherKinds

object CurationGenerators {

  implicit def curatedTriplesObjects[Interpretation[_]]: Gen[CuratedTriples[Interpretation]] = curatedTriplesObjects(
    nonEmptyList(curationUpdateFunctions).map(_.toList)
  )

  def curatedTriplesObjects[Interpretation[_]](
      updatesGenerator: Gen[List[UpdateFunction[Interpretation]]]
  ): Gen[CuratedTriples[Interpretation]] =
    for {
      triples <- jsonLDTriples
      updates <- updatesGenerator
    } yield CuratedTriples(triples, updates)

  def curatedTriplesObjects[Interpretation[_]](triples: JsonLD): Gen[CuratedTriples[Interpretation]] =
    for {
      updates <- nonEmptyList(curationUpdateFunctions)
    } yield CuratedTriples(JsonLDTriples(List(triples.toJson)), updates.toList)

  implicit def curationUpdateFunctions[Interpretation[_]](
      implicit ME: MonadError[Interpretation, Throwable]
  ): Gen[UpdateFunction[Interpretation]] =
    for {
      name        <- nonBlankStrings(minLength = 5)
      sparqlQuery <- sentences() map (v => SparqlQuery("curation update", Set.empty, v.value))
    } yield UpdateFunction(name, () => sparqlQuery.pure[Interpretation])
}

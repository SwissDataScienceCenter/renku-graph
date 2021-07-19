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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration

import cats.MonadThrow
import cats.data.EitherT
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationData
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationData.{ResultData, TransformationStep}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators.projectMetadatas
import eu.timepit.refined.auto._
import io.renku.jsonld.JsonLD
import org.scalacheck.Gen

private[triplesgenerated] object CurationGenerators {

  implicit def transformationDataObjects[Interpretation[_]: MonadThrow]: Gen[TransformationData[Interpretation]] =
    transformationDataObjects[Interpretation](
      transformationSteps[Interpretation].generateNonEmptyList().toList
    )

  def transformationDataObjects[Interpretation[_]: MonadThrow](
      transformationStepsGenerator: Gen[List[TransformationStep[Interpretation]]]
  ): Gen[TransformationData[Interpretation]] = for {
    triples  <- jsonLDTriples
    metadata <- projectMetadatas
    updates  <- transformationStepsGenerator
  } yield TransformationData[Interpretation](triples, metadata, updates)

  def curatedTriplesObjects[Interpretation[_]: MonadThrow](triples: JsonLD): Gen[TransformationData[Interpretation]] =
    for {
      updates  <- nonEmptyList(transformationSteps[Interpretation])
      metadata <- projectMetadatas
    } yield TransformationData(JsonLDTriples(triples.flatten.fold(throw _, identity).toJson), metadata, updates.toList)

  implicit def transformationSteps[Interpretation[_]: MonadThrow]: Gen[TransformationStep[Interpretation]] = for {
    name          <- nonBlankStrings(minLength = 5)
    sparqlQueries <- sparqlQueries.toGeneratorOfList()
  } yield TransformationStep[Interpretation](
    name,
    projectMetadata => EitherT.rightT(ResultData(projectMetadata, sparqlQueries))
  )
}

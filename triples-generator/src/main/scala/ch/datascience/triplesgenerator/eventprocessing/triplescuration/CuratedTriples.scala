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
import cats.data.EitherT
import ch.datascience.rdfstore.{CypherQuery, GraphQuery, JsonLDTriples, SparqlQuery}
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.CurationUpdatesGroup
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty

import scala.language.higherKinds

final case class CuratedTriples[Interpretation[_], Q <: GraphQuery](
    triples:       JsonLDTriples,
    updatesGroups: List[CurationUpdatesGroup[Interpretation, Q]]
)

object CuratedTriples {
  private[eventprocessing] type GeneratedQueries[Interpretation[_], Q <: GraphQuery] =
    EitherT[Interpretation, ProcessingRecoverableError, List[Q]]

  final case class CurationUpdatesGroup[Interpretation[_], Q <: GraphQuery](
      name:           String Refined NonEmpty,
      queryGenerator: () => GeneratedQueries[Interpretation, Q]
  ) {
    def generateUpdates(): GeneratedQueries[Interpretation, Q] = queryGenerator()
  }

  object CurationUpdatesGroup {
    def apply[Interpretation[_], Q <: GraphQuery](
        name:          String Refined NonEmpty,
        sparqlQueries: Q*
    )(implicit ME:     MonadError[Interpretation, Throwable]): CurationUpdatesGroup[Interpretation, Q] =
      CurationUpdatesGroup[Interpretation, Q](
        name,
        () => EitherT.rightT[Interpretation, ProcessingRecoverableError](sparqlQueries.toList)
      )

  }

}

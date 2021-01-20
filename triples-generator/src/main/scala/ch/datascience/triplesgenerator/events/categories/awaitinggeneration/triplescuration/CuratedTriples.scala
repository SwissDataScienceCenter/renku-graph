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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration

import cats.MonadError
import cats.data.EitherT
import ch.datascience.rdfstore.{JsonLDTriples, SparqlQuery}
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.CuratedTriples.CurationUpdatesGroup
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty

final case class CuratedTriples[Interpretation[_]](triples:       JsonLDTriples,
                                                   updatesGroups: List[CurationUpdatesGroup[Interpretation]]
)

object CuratedTriples {
  private[awaitinggeneration] type GeneratedQueries[Interpretation[_]] =
    EitherT[Interpretation, ProcessingRecoverableError, List[SparqlQuery]]

  final case class CurationUpdatesGroup[Interpretation[_]](
      name:           String Refined NonEmpty,
      queryGenerator: () => GeneratedQueries[Interpretation]
  ) {
    def generateUpdates(): GeneratedQueries[Interpretation] = queryGenerator()
  }

  object CurationUpdatesGroup {
    def apply[Interpretation[_]](
        name:          String Refined NonEmpty,
        sparqlQueries: SparqlQuery*
    )(implicit ME:     MonadError[Interpretation, Throwable]): CurationUpdatesGroup[Interpretation] =
      CurationUpdatesGroup[Interpretation](
        name,
        () => EitherT.rightT[Interpretation, ProcessingRecoverableError](sparqlQueries.toList)
      )

  }

}

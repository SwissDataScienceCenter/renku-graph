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
import ch.datascience.rdfstore.{JsonLDTriples, SparqlQuery}
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.UpdateFunction

import scala.language.higherKinds

final case class CuratedTriples[Interpretation[_]](triples: JsonLDTriples,
                                                   updates: List[UpdateFunction[Interpretation]])

object CuratedTriples {
  private[eventprocessing] type UpdateResult[Interpretation[_]] =
    EitherT[Interpretation, ProcessingRecoverableError, SparqlQuery]

  final case class UpdateFunction[Interpretation[_]](name: String, queryGenerator: () => UpdateResult[Interpretation])
      extends (() => UpdateResult[Interpretation]) {
    override def apply(): UpdateResult[Interpretation] = queryGenerator()
  }

  object UpdateFunction {
    def apply[Interpretation[_]](name: String, sparqlQuery: SparqlQuery)(
        implicit ME:                   MonadError[Interpretation, Throwable]
    ): UpdateFunction[Interpretation] =
      UpdateFunction[Interpretation](name,
                                     () => EitherT.rightT[Interpretation, ProcessingRecoverableError](sparqlQuery))
  }

}

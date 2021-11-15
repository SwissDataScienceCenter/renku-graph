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

package io.renku.triplesgenerator.events.categories.triplesgenerated

import cats.data.EitherT
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.graph.model.entities.Project
import io.renku.rdfstore.SparqlQuery
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{Transformation, TransformationStepResult}

private[triplesgenerated] final case class TransformationStep[F[_]](
    name:           String Refined NonEmpty,
    transformation: Transformation[F]
) {
  def run(project: Project): TransformationStepResult[F] = transformation(project)
}

private object TransformationStep {

  private[triplesgenerated] type Transformation[F[_]] = Project => TransformationStepResult[F]

  private[triplesgenerated] type TransformationStepResult[F[_]] =
    EitherT[F, ProcessingRecoverableError, ResultData]

  private[triplesgenerated] final case class ResultData(project: Project, queries: List[SparqlQuery])

  object ResultData {
    def apply(project: Project): ResultData = ResultData(project, queries = Nil)
  }
}

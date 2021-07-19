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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.data.EitherT
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{Transformation, TransformationStepResult}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty

private[triplesgenerated] final case class TransformationStep[Interpretation[_]](
    name:           String Refined NonEmpty,
    transformation: Transformation[Interpretation]
) {
  def run(projectMetadata: ProjectMetadata): TransformationStepResult[Interpretation] =
    transformation(projectMetadata)
}

private object TransformationStep {

  private[triplesgenerated] type Transformation[Interpretation[_]] =
    ProjectMetadata => TransformationStepResult[Interpretation]

  private[triplesgenerated] type TransformationStepResult[Interpretation[_]] =
    EitherT[Interpretation, ProcessingRecoverableError, ResultData]

  private[triplesgenerated] final case class ResultData(
      projectMetadata: ProjectMetadata,
      queries:         List[SparqlQuery]
  )

  object ResultData {
    def apply(projectMetadata: ProjectMetadata): ResultData = ResultData(projectMetadata, queries = Nil)
  }
}

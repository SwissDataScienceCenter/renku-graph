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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.DatasetTransformer
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails.PersonTransformer
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects.ProjectTransformer
import org.typelevel.log4cats.Logger

private[triplesgenerated] trait TransformationStepsCreator[F[_]] {
  def createSteps: List[TransformationStep[F]]
}

private[triplesgenerated] class TransformationStepsCreatorImpl[F[_]: MonadThrow](
    personTransformer:  PersonTransformer[F],
    projectTransformer: ProjectTransformer[F],
    datasetTransformer: DatasetTransformer[F]
) extends TransformationStepsCreator[F] {

  override def createSteps: List[TransformationStep[F]] = List(
    personTransformer.createTransformationStep,
    projectTransformer.createTransformationStep,
    datasetTransformer.createTransformationStep
  )
}

private[triplesgenerated] object TransformationStepsCreator {

  final case class TransformationRecoverableError(message: String, cause: Throwable)
      extends Exception(message, cause)
      with ProcessingRecoverableError

  object TransformationRecoverableError {
    def apply(message: String): TransformationRecoverableError = TransformationRecoverableError(message, null)
  }

  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[TransformationStepsCreator[F]] = for {
    personTransformer  <- PersonTransformer(timeRecorder)
    projectTransformer <- ProjectTransformer(timeRecorder)
    datasetTransformer <- DatasetTransformer(timeRecorder)
  } yield new TransformationStepsCreatorImpl[F](personTransformer, projectTransformer, datasetTransformer)
}

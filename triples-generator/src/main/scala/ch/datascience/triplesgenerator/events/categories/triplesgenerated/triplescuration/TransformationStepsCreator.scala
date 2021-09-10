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
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets.DatasetTransformer
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails.PersonTransformer
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects.ProjectTransformer
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private[triplesgenerated] trait TransformationStepsCreator[Interpretation[_]] {
  def createSteps: List[TransformationStep[Interpretation]]
}

private[triplesgenerated] class TransformationStepsCreatorImpl[Interpretation[_]: MonadThrow](
    personTransformer:  PersonTransformer[Interpretation],
    projectTransformer: ProjectTransformer[Interpretation],
    datasetTransformer: DatasetTransformer[Interpretation]
) extends TransformationStepsCreator[Interpretation] {

  override def createSteps: List[TransformationStep[Interpretation]] = List(
    personTransformer.createTransformationStep,
    projectTransformer.createTransformationStep,
    datasetTransformer.createTransformationStep
  )
}

private[triplesgenerated] object TriplesCurator {

  import cats.effect.{ContextShift, IO, Timer}

  final case class TransformationRecoverableError(message: String, cause: Throwable)
      extends Exception(message, cause)
      with ProcessingRecoverableError

  object TransformationRecoverableError {
    def apply(message: String): TransformationRecoverableError = TransformationRecoverableError(message, null)
  }

  def apply(
      logger:       Logger[IO],
      timeRecorder: SparqlQueryTimeRecorder[IO]
  )(implicit
      executionContext: ExecutionContext,
      cs:               ContextShift[IO],
      timer:            Timer[IO]
  ): IO[TransformationStepsCreator[IO]] =
    for {
      personTransformer  <- PersonTransformer(timeRecorder, logger)
      projectTransformer <- ProjectTransformer(timeRecorder, logger)
      datasetTransformer <- DatasetTransformer(timeRecorder, logger)
    } yield new TransformationStepsCreatorImpl[IO](
      personTransformer,
      projectTransformer,
      datasetTransformer
    )
}

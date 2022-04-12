/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.activities.ActivityTransformer
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.datasets.DatasetTransformer
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.persondetails.PersonTransformer
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.projects.ProjectTransformer
import org.typelevel.log4cats.Logger

private[triplesgenerated] trait TransformationStepsCreator[F[_]] {
  def createSteps: List[TransformationStep[F]]
}

private[triplesgenerated] class TransformationStepsCreatorImpl[F[_]: MonadThrow](
    personTransformer:   PersonTransformer[F],
    projectTransformer:  ProjectTransformer[F],
    datasetTransformer:  DatasetTransformer[F],
    activityTransformer: ActivityTransformer[F]
) extends TransformationStepsCreator[F] {

  override def createSteps: List[TransformationStep[F]] = List(
    personTransformer.createTransformationStep,
    projectTransformer.createTransformationStep,
    datasetTransformer.createTransformationStep,
    activityTransformer.createTransformationStep
  )
}

private[triplesgenerated] object TransformationStepsCreator {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[TransformationStepsCreator[F]] = for {
    personTransformer   <- PersonTransformer[F]
    projectTransformer  <- ProjectTransformer[F]
    datasetTransformer  <- DatasetTransformer[F]
    activityTransformer <- ActivityTransformer[F]
  } yield new TransformationStepsCreatorImpl[F](personTransformer,
                                                projectTransformer,
                                                datasetTransformer,
                                                activityTransformer
  )
}

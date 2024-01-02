/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.tsprovisioning
package transformation

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.triplesgenerator.tsprovisioning.TransformationStep
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private[tsprovisioning] trait TransformationStepsCreator[F[_]] {
  def createSteps: List[TransformationStep[F]]
}

private[tsprovisioning] class TransformationStepsCreatorImpl[F[_]: MonadThrow](
    personTransformer:   namedgraphs.persons.PersonTransformer[F],
    projectTransformer:  namedgraphs.projects.ProjectTransformer[F],
    datasetTransformer:  namedgraphs.datasets.DatasetTransformer[F],
    planTransformer:     namedgraphs.plans.PlanTransformer[F],
    activityTransformer: namedgraphs.activities.ActivityTransformer[F]
) extends TransformationStepsCreator[F] {

  override def createSteps: List[TransformationStep[F]] = List(
    personTransformer.createTransformationStep,
    projectTransformer.createTransformationStep,
    datasetTransformer.createTransformationStep,
    planTransformer.createTransformationStep,
    activityTransformer.createTransformationStep
  )
}

private[tsprovisioning] object TransformationStepsCreator {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[TransformationStepsCreator[F]] = for {
    personTransformer   <- namedgraphs.persons.PersonTransformer[F]
    projectTransformer  <- namedgraphs.projects.ProjectTransformer[F]
    datasetTransformer  <- namedgraphs.datasets.DatasetTransformer[F]
    planTransformer     <- namedgraphs.plans.PlanTransformer[F]
    activityTransformer <- namedgraphs.activities.ActivityTransformer[F]
  } yield new TransformationStepsCreatorImpl[F](personTransformer,
                                                projectTransformer,
                                                datasetTransformer,
                                                planTransformer,
                                                activityTransformer
  )
}

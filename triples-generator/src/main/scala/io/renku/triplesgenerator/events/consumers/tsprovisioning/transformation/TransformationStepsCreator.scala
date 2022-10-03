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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning
package transformation

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.TSVersion
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private[consumers] trait TransformationStepsCreator[F[_]] {
  def createSteps(tsVersion: TSVersion): List[TransformationStep[F]]
}

private[tsprovisioning] class TransformationStepsCreatorImpl[F[_]: MonadThrow](
    defaultGraphPersonTransformer:   defaultgraph.persons.PersonTransformer[F],
    defaultGraphProjectTransformer:  defaultgraph.projects.ProjectTransformer[F],
    defaultGraphDatasetTransformer:  defaultgraph.datasets.DatasetTransformer[F],
    defaultGraphActivityTransformer: defaultgraph.activities.ActivityTransformer[F],
    namedGraphsPersonTransformer:    namedgraphs.persons.PersonTransformer[F],
    namedGraphsProjectTransformer:   namedgraphs.projects.ProjectTransformer[F],
    namedGraphsDatasetTransformer:   namedgraphs.datasets.DatasetTransformer[F],
    namedGraphsActivityTransformer:  namedgraphs.activities.ActivityTransformer[F]
) extends TransformationStepsCreator[F] {

  override def createSteps(tsVersion: TSVersion): List[TransformationStep[F]] = tsVersion match {
    case TSVersion.DefaultGraph =>
      List(
        defaultGraphPersonTransformer.createTransformationStep,
        defaultGraphProjectTransformer.createTransformationStep,
        defaultGraphDatasetTransformer.createTransformationStep,
        defaultGraphActivityTransformer.createTransformationStep
      )
    case TSVersion.NamedGraphs =>
      List(
        namedGraphsPersonTransformer.createTransformationStep,
        namedGraphsProjectTransformer.createTransformationStep,
        namedGraphsDatasetTransformer.createTransformationStep,
        namedGraphsActivityTransformer.createTransformationStep
      )
  }
}

private[consumers] object TransformationStepsCreator {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[TransformationStepsCreator[F]] = for {
    defaultGraphPersonTransformer   <- defaultgraph.persons.PersonTransformer[F]
    defaultGraphProjectTransformer  <- defaultgraph.projects.ProjectTransformer[F]
    defaultGraphDatasetTransformer  <- defaultgraph.datasets.DatasetTransformer[F]
    defaultGraphActivityTransformer <- defaultgraph.activities.ActivityTransformer[F]
    namedGraphsPersonTransformer    <- namedgraphs.persons.PersonTransformer[F]
    namedGraphsProjectTransformer   <- namedgraphs.projects.ProjectTransformer[F]
    namedGraphsDatasetTransformer   <- namedgraphs.datasets.DatasetTransformer[F]
    namedGraphsActivityTransformer  <- namedgraphs.activities.ActivityTransformer[F]
  } yield new TransformationStepsCreatorImpl[F](defaultGraphPersonTransformer,
                                                defaultGraphProjectTransformer,
                                                defaultGraphDatasetTransformer,
                                                defaultGraphActivityTransformer,
                                                namedGraphsPersonTransformer,
                                                namedGraphsProjectTransformer,
                                                namedGraphsDatasetTransformer,
                                                namedGraphsActivityTransformer
  )
}

/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
package transformation.namedgraphs.plans

import TransformationStep.{Queries, Transformation}
import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.entities.Project
import io.renku.triplesgenerator.errors.{ProcessingRecoverableError, RecoverableErrorsRecovery}
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private[transformation] trait PlanTransformer[F[_]] {
  def createTransformationStep: TransformationStep[F]
}

private[transformation] object PlanTransformer {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[PlanTransformer[F]] =
    KGInfoFinder[F].map(new PlanTransformerImpl[F](_, UpdatesCreator))
}

private class PlanTransformerImpl[F[_]: MonadThrow](
    kgInfoFinder:              KGInfoFinder[F],
    updatesCreator:            UpdatesCreator,
    recoverableErrorsRecovery: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends PlanTransformer[F] {

  import eu.timepit.refined.auto._
  import io.renku.graph.model.entities.ProjectLens._
  import kgInfoFinder._
  import recoverableErrorsRecovery._
  import updatesCreator._

  override def createTransformationStep: TransformationStep[F] =
    TransformationStep("Plan Updates", createTransformation)

  private def createTransformation: Transformation[F] = project =>
    EitherT {
      updateDateCreated(project -> Queries.empty)
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeRecoverableError("Problem finding activity details in KG"))
    }

  private lazy val updateDateCreated: ((Project, Queries)) => F[(Project, Queries)] = { case (project, queries) =>
    collectStepPlans(project.plans)
      .map { plan =>
        findCreatedDates(project.resourceId, plan.resourceId).map {
          queriesDeletingDate(project.resourceId, plan, _)
        }
      }
      .sequence
      .map(_.flatten)
      .map(quers => project -> (queries |+| Queries.preDataQueriesOnly(quers)))
  }
}
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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation
package projects

import cats.MonadThrow
import cats.data.EitherT
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.tsprovisioning.TransformationStep.{Queries, Transformation}
import io.renku.triplesgenerator.events.consumers.tsprovisioning.{RecoverableErrorsRecovery, TransformationStep}
import org.typelevel.log4cats.Logger

private[transformation] trait ProjectTransformer[F[_]] {
  def createTransformationStep: TransformationStep[F]
}

private class ProjectTransformerImpl[F[_]: MonadThrow](
    kgProjectFinder:           KGProjectFinder[F],
    updatesCreator:            UpdatesCreator,
    recoverableErrorsRecovery: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ProjectTransformer[F] {
  import recoverableErrorsRecovery._
  import updatesCreator._

  override def createTransformationStep: TransformationStep[F] =
    TransformationStep("Project Details Updates", createTransformation)

  private def createTransformation: Transformation[F] = project =>
    EitherT {
      kgProjectFinder
        .find(project.resourceId)
        .map {
          case None => (project, Queries.empty).asRight[ProcessingRecoverableError]
          case Some(kgData) =>
            (project, Queries.preDataQueriesOnly(prepareUpdates(project, kgData)))
              .asRight[ProcessingRecoverableError]
        }
        .recoverWith(maybeRecoverableError("Problem finding project details in KG"))
    }
}

private[transformation] object ProjectTransformer {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProjectTransformer[F]] = for {
    kgProjectFinder <- KGProjectFinder[F]
  } yield new ProjectTransformerImpl[F](kgProjectFinder, UpdatesCreator)
}

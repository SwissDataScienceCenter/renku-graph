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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.projects

import cats.MonadThrow
import cats.data.EitherT
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.entities.Project
import io.renku.triplesgenerator.errors.{ProcessingRecoverableError, RecoverableErrorsRecovery}
import io.renku.triplesgenerator.tsprovisioning.TransformationStep
import io.renku.triplesgenerator.tsprovisioning.TransformationStep.{Queries, Transformation}
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

private[transformation] trait ProjectTransformer[F[_]] {
  def createTransformationStep: TransformationStep[F]
}

private class ProjectTransformerImpl[F[_]: MonadThrow](
    kgProjectFinder:           KGProjectFinder[F],
    updatesCreator:            UpdatesCreator,
    dateCreatedUpdater:        DateCreatedUpdater,
    dateModifiedUpdater:       DateModifiedUpdater,
    recoverableErrorsRecovery: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ProjectTransformer[F] {
  import dateCreatedUpdater.updateDateCreated
  import dateModifiedUpdater.updateDateModified
  import recoverableErrorsRecovery._
  import updatesCreator._

  override def createTransformationStep: TransformationStep[F] =
    TransformationStep("Project Details Updates", createTransformation)

  private def createTransformation: Transformation[F] = project =>
    EitherT {
      kgProjectFinder
        .find(project.resourceId)
        .map {
          case None         => (project, Queries.empty).asRight[ProcessingRecoverableError]
          case Some(kgData) => transform(project, kgData).asRight[ProcessingRecoverableError]
        }
        .recoverWith(maybeRecoverableError("Problem finding project details in KG"))
    }

  private def transform(project: Project, kgData: ProjectMutableData): (Project, Queries) = (
    updateDateCreated(kgData) >>> updateDateModified(kgData) >>> addOtherUpdates(kgData)
  )(project, Queries.empty)

  private def addOtherUpdates(kgData: ProjectMutableData): ((Project, Queries)) => (Project, Queries) = {
    case (project, queries) =>
      (project, queries ++ Queries(prepareUpdates(project, kgData), postUpdates(project)))
  }
}

private[transformation] object ProjectTransformer {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProjectTransformer[F]] =
    KGProjectFinder[F].map(
      new ProjectTransformerImpl[F](_, UpdatesCreator, DateCreatedUpdater(), DateModifiedUpdater())
    )
}

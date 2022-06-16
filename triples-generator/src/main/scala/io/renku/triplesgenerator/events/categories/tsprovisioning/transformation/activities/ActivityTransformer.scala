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

package io.renku.triplesgenerator.events.categories.tsprovisioning.transformation
package activities

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.entities.Project
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.tsprovisioning.TransformationStep.{Queries, Transformation}
import io.renku.triplesgenerator.events.categories.tsprovisioning.{RecoverableErrorsRecovery, TransformationStep}
import org.typelevel.log4cats.Logger

private[transformation] trait ActivityTransformer[F[_]] {
  def createTransformationStep: TransformationStep[F]
}

private[transformation] object ActivityTransformer {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ActivityTransformer[F]] = for {
    kgInfoFinder <- KGInfoFinder[F]
  } yield new ActivityTransformerImpl[F](kgInfoFinder, UpdatesCreator)
}

private[transformation] class ActivityTransformerImpl[F[_]: MonadThrow](
    kgInfoFinder:              KGInfoFinder[F],
    updatesCreator:            UpdatesCreator,
    recoverableErrorsRecovery: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ActivityTransformer[F] {

  import eu.timepit.refined.auto._
  import kgInfoFinder._
  import recoverableErrorsRecovery._
  import updatesCreator._

  override def createTransformationStep: TransformationStep[F] =
    TransformationStep("Activity Updates", createTransformation)

  private def createTransformation: Transformation[F] = project =>
    EitherT {
      (updateAuthorLinks(project -> Queries.empty) >>= updateAssociationAgentLinks)
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeRecoverableError("Problem finding activity details in KG"))
    }

  private lazy val updateAuthorLinks: ((Project, Queries)) => F[(Project, Queries)] = { case (project, queries) =>
    project.activities
      .map(activity => findActivityAuthors(activity.resourceId).map(queriesUnlinkingAuthors(activity, _)))
      .sequence
      .map(_.flatten)
      .map(quers => project -> (queries |+| Queries.preDataQueriesOnly(quers)))
  }

  private lazy val updateAssociationAgentLinks: ((Project, Queries)) => F[(Project, Queries)] = {
    case (project, queries) =>
      project.activities
        .map(activity => findAssociationPersonAgents(activity.resourceId).map(queriesUnlinkingAgents(activity, _)))
        .sequence
        .map(_.flatten)
        .map(quers => project -> (queries |+| Queries.preDataQueriesOnly(quers)))
  }
}

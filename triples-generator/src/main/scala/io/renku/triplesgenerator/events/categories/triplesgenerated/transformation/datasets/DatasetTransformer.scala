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
package datasets

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{Queries, Transformation}
import io.renku.triplesgenerator.events.categories.triplesgenerated.{RecoverableErrorsRecovery, TransformationStep}
import org.typelevel.log4cats.Logger

private[transformation] trait DatasetTransformer[F[_]] {
  def createTransformationStep: TransformationStep[F]
}

private[transformation] class DatasetTransformerImpl[F[_]: MonadThrow](
    derivationHierarchyUpdater:     DerivationHierarchyUpdater[F],
    topmostSameAsUpdater:           TopmostSameAsUpdater[F],
    initialVersionsUpdater:         InitialVersionsUpdater[F],
    personLinksUpdater:             PersonLinksUpdater[F],
    hierarchyOnInvalidationUpdater: HierarchyOnInvalidationUpdater[F],
    recoverableErrorsRecovery:      RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends DatasetTransformer[F] {

  import derivationHierarchyUpdater._
  import hierarchyOnInvalidationUpdater._
  import initialVersionsUpdater._
  import personLinksUpdater._
  import recoverableErrorsRecovery._
  import topmostSameAsUpdater._

  override def createTransformationStep: TransformationStep[F] =
    TransformationStep("Dataset Details Updates", createTransformation)

  private def createTransformation: Transformation[F] = project =>
    EitherT {
      (fixDerivationHierarchies(project -> Queries.empty) >>=
        updateTopmostSameAs >>=
        updateInitialVersions >>=
        updatePersonLinks >>=
        updateHierarchyOnInvalidation)
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeRecoverableError("Problem finding dataset details in KG"))
    }
}

private[transformation] object DatasetTransformer {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[DatasetTransformer[F]] = for {
    derivationHierarchyUpdater     <- DerivationHierarchyUpdater[F]
    topmostSameAsUpdater           <- TopmostSameAsUpdater[F]
    personLinksUpdater             <- PersonLinksUpdater[F]
    hierarchyOnInvalidationUpdater <- HierarchyOnInvalidationUpdater[F]
    initialVersionsUpdater         <- InitialVersionsUpdater[F]
  } yield new DatasetTransformerImpl[F](derivationHierarchyUpdater,
                                        topmostSameAsUpdater,
                                        initialVersionsUpdater,
                                        personLinksUpdater,
                                        hierarchyOnInvalidationUpdater
  )
}

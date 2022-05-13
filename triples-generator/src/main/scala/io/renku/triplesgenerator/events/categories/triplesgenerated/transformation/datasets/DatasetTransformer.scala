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
import io.renku.graph.model.entities.{Dataset, Project}
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{Queries, Transformation}
import io.renku.triplesgenerator.events.categories.triplesgenerated.{ProjectFunctions, RecoverableErrorsRecovery, TransformationStep}
import org.typelevel.log4cats.Logger

private[transformation] trait DatasetTransformer[F[_]] {
  def createTransformationStep: TransformationStep[F]
}

private[transformation] class DatasetTransformerImpl[F[_]: MonadThrow](
    kgDatasetInfoFinder:       KGDatasetInfoFinder[F],
    updatesCreator:            UpdatesCreator,
    projectFunctions:          ProjectFunctions,
    recoverableErrorsRecovery: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends DatasetTransformer[F] {

  import kgDatasetInfoFinder._
  import projectFunctions._
  import recoverableErrorsRecovery._

  override def createTransformationStep: TransformationStep[F] =
    TransformationStep("Dataset Details Updates", createTransformation)

  private def createTransformation: Transformation[F] = project =>
    EitherT {
      (fixDerivationHierarchies(project -> Queries.empty) >>=
        updateTopmostSameAs >>=
        updatePersonLinks >>=
        updateHierarchyOnInvalidation)
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeRecoverableError("Problem finding dataset details in KG"))
    }

  private lazy val fixDerivationHierarchies: ((Project, Queries)) => F[(Project, Queries)] = {
    case (project, queries) =>
      findModifiedDatasets(project)
        .foldLeft((project -> queries).pure[F]) { (projectAndQueriesF, originalDS) =>
          projectAndQueriesF >>= { case (proj, quers) =>
            findTopmostDerivedFrom[F](originalDS, proj)
              .map(originalDS.update)
              .map(updatedDS => update(originalDS, updatedDS)(proj) -> updatedDS)
              .map { case (updatedProj, updatedDS) =>
                val deletions = updatesCreator.deleteOtherTopmostDerivedFrom(updatedDS)
                updatedProj -> (quers |+| Queries.postDataQueriesOnly(deletions))
              }
          }
        }
  }

  private lazy val updateTopmostSameAs: ((Project, Queries)) => F[(Project, Queries)] = { case (project, queries) =>
    findInternallyImportedDatasets(project)
      .foldLeft((project -> queries).pure[F]) { (projectAndQueriesF, dataset) =>
        for {
          projectAndQueries        <- projectAndQueriesF
          maybeParentTopmostSameAs <- findParentTopmostSameAs(dataset.provenance.sameAs)
          maybeKGTopmostSameAs     <- findTopmostSameAs(dataset.identification.resourceId)
          updatedDataset = maybeParentTopmostSameAs.map(dataset.update) getOrElse dataset
        } yield (
          update(dataset, updatedDataset)(projectAndQueries._1),
          projectAndQueries._2 |+| Queries.postDataQueriesOnly(
            updatesCreator.prepareUpdates(dataset, maybeKGTopmostSameAs) :::
              updatesCreator.prepareTopmostSameAsCleanup(updatedDataset, maybeParentTopmostSameAs)
          )
        )
      }
  }

  private lazy val updatePersonLinks: ((Project, Queries)) => F[(Project, Queries)] = { case (project, queries) =>
    project.datasets
      .map(ds => findDatasetCreators(ds.resourceId).map(updatesCreator.queriesUnlinkingCreators(ds, _)))
      .sequence
      .map(_.flatten)
      .map(quers => project -> (queries |+| Queries.preDataQueriesOnly(quers)))
  }

  private lazy val updateHierarchyOnInvalidation: ((Project, Queries)) => F[(Project, Queries)] = {
    case (project, queries) =>
      findInvalidatedDatasets(project)
        .foldLeft((project -> queries).pure[F]) { (projectAndQueriesF, dataset) =>
          projectAndQueriesF map { case (project, queries) =>
            val preDataUploadQueries = dataset.provenance match {
              case _: Dataset.Provenance.Internal =>
                updatesCreator.prepareUpdatesWhenInvalidated(
                  dataset.asInstanceOf[Dataset[Dataset.Provenance.Internal]]
                )
              case _: Dataset.Provenance.ImportedExternal =>
                updatesCreator.prepareUpdatesWhenInvalidated(
                  dataset.asInstanceOf[Dataset[Dataset.Provenance.ImportedExternal]]
                )
              case _: Dataset.Provenance.ImportedInternal =>
                updatesCreator.prepareUpdatesWhenInvalidated(
                  dataset.asInstanceOf[Dataset[Dataset.Provenance.ImportedInternal]]
                )
              case _ => Nil
            }
            project -> (queries |+| Queries.postDataQueriesOnly(preDataUploadQueries))
          }
        }
  }
}

private[transformation] object DatasetTransformer {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[DatasetTransformer[F]] = for {
    kgDatasetInfoFinder <- KGDatasetInfoFinder[F]
  } yield new DatasetTransformerImpl[F](kgDatasetInfoFinder, UpdatesCreator, ProjectFunctions)
}

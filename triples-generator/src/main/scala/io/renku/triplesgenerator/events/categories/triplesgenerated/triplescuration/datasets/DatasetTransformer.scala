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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.entities.Dataset
import io.renku.http.client.RestClientError._
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{ResultData, Transformation}
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.{ProjectFunctions, TransformationStep}
import org.typelevel.log4cats.Logger

private[triplescuration] trait DatasetTransformer[F[_]] {
  def createTransformationStep: TransformationStep[F]
}

private[triplescuration] class DatasetTransformerImpl[F[_]: MonadThrow](
    kgDatasetInfoFinder: KGDatasetInfoFinder[F],
    updatesCreator:      UpdatesCreator,
    projectFunctions:    ProjectFunctions
) extends DatasetTransformer[F] {

  import kgDatasetInfoFinder._
  import projectFunctions._

  override def createTransformationStep: TransformationStep[F] =
    TransformationStep("Dataset Details Updates", createTransformation)

  private def createTransformation: Transformation[F] = project =>
    EitherT {
      (updateTopmostSameAs(ResultData(project)) >>= updateTopmostDerivedFrom >>= updateHierarchyOnInvalidation)
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeToRecoverableError)
    }

  private def updateTopmostSameAs(resultData: ResultData) = findInternallyImportedDatasets(resultData.project)
    .foldLeft(resultData.pure[F]) { (resultDataF, dataset) =>
      for {
        resultData               <- resultDataF
        maybeParentTopmostSameAs <- findParentTopmostSameAs(dataset.provenance.sameAs)
        maybeKGTopmostSameAs     <- findTopmostSameAs(dataset.identification.resourceId)
        updatedDataset = maybeParentTopmostSameAs.map(dataset.update) getOrElse dataset
      } yield ResultData(
        project = update(dataset, updatedDataset)(resultData.project),
        queries = resultData.queries ::: updatesCreator.prepareUpdates(dataset, maybeKGTopmostSameAs)
      )
    }

  private def updateTopmostDerivedFrom(resultData: ResultData) =
    findModifiedDatasets(resultData.project)
      .foldLeft(resultData.pure[F]) { (resultDataF, dataset) =>
        for {
          resultData                    <- resultDataF
          maybeParentTopmostDerivedFrom <- findParentTopmostDerivedFrom(dataset.provenance.derivedFrom)
          maybeKGTopmostDerivedFrom     <- findTopmostDerivedFrom(dataset.identification.resourceId)
          updatedDataset = maybeParentTopmostDerivedFrom.map(dataset.update) getOrElse dataset
        } yield ResultData(
          project = update(dataset, updatedDataset)(resultData.project),
          queries = resultData.queries ::: updatesCreator.prepareUpdates(dataset, maybeKGTopmostDerivedFrom)
        )
      }

  private def updateHierarchyOnInvalidation(resultData: ResultData) = findInvalidatedDatasets(resultData.project)
    .foldLeft(resultData.pure[F]) { (resultDataF, dataset) =>
      for {
        resultData <- resultDataF
        queries = dataset.provenance match {
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
      } yield ResultData(
        project = resultData.project,
        queries = resultData.queries ::: queries
      )
    }

  private lazy val maybeToRecoverableError
      : PartialFunction[Throwable, F[Either[ProcessingRecoverableError, ResultData]]] = {
    case e @ (_: UnexpectedResponseException | _: ConnectivityException | _: ClientException |
        _: UnauthorizedException) =>
      TransformationRecoverableError("Problem finding dataset details in KG", e)
        .asLeft[ResultData]
        .leftWiden[ProcessingRecoverableError]
        .pure[F]
  }
}

private[triplescuration] object DatasetTransformer {

  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[DatasetTransformer[F]] = for {
    kgDatasetInfoFinder <- KGDatasetInfoFinder(timeRecorder)
  } yield new DatasetTransformerImpl[F](kgDatasetInfoFinder, UpdatesCreator, ProjectFunctions)
}

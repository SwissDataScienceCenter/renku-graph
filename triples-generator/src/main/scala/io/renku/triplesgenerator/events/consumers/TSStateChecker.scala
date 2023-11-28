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

package io.renku.triplesgenerator.events.consumers

import TSStateChecker.TSState
import cats.{MonadThrow, Show}
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.triplesstore.DatasetTTLs
import tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import io.renku.triplesstore.{DatasetName, SparqlQueryTimeRecorder, TSAdminClient}
import org.typelevel.log4cats.Logger

private trait TSStateChecker[F[_]] {
  def checkTSState: F[TSState]
}

private object TSStateChecker {
  sealed trait TSState extends Product with Serializable
  object TSState {
    case object Ready           extends TSState
    case object ReProvisioning  extends TSState
    case object Migrating       extends TSState
    case object MissingDatasets extends TSState
  }

  def apply[F[_]: Async: ReProvisioningStatus: Logger: SparqlQueryTimeRecorder]: F[TSStateChecker[F]] =
    TSAdminClient[F].map(
      new TSStateCheckerImpl(DatasetTTLs.allFactories.map(_.datasetName), _, ReProvisioningStatus[F])
    )

  implicit val show: Show[TSState] = Show.show {
    case TSState.Ready           => "Ready"
    case TSState.ReProvisioning  => "Re-provisioning running"
    case TSState.Migrating       => "Migration running"
    case TSState.MissingDatasets => "Not all datasets created"
  }
}

private class TSStateCheckerImpl[F[_]: MonadThrow](
    datasets:             List[DatasetName],
    tsAdminClient:        TSAdminClient[F],
    reProvisioningStatus: ReProvisioningStatus[F]
) extends TSStateChecker[F] {

  import reProvisioningStatus.underReProvisioning

  override def checkTSState: F[TSState] =
    checkDatasetsExist >>= {
      case true =>
        underReProvisioning() map {
          case true  => TSState.ReProvisioning
          case false => TSState.Ready
        }
      case false => TSState.MissingDatasets.pure[F].widen
    }

  private def checkDatasetsExist =
    datasets
      .map(tsAdminClient.checkDatasetExists)
      .sequence
      .map(_.reduce(_ && _))
}

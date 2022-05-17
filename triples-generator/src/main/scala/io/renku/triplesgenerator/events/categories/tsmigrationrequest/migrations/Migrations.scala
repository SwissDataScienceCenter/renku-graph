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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger
import reprovisioning.{ReProvisioning, ReProvisioningStatus}

private[tsmigrationrequest] object Migrations {

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      reProvisioningStatus: ReProvisioningStatus[F],
      config:               Config
  ): F[List[Migration[F]]] = for {
    reProvisioning                    <- ReProvisioning[F](reProvisioningStatus, config)
    malformedActivityIds              <- MalformedActivityIds[F]
    multiplePersonNames               <- MultiplePersonNames[F]
    multipleModifiedDSData            <- MultipleModifiedDSData[F]
    malformedDSImageIds               <- MalformedDSImageIds[F]
    multipleDSTopmostSameAs           <- MultipleDSTopmostSameAs[F]
    multipleAllWrongTopmostSameAs     <- MultipleAllWrongTopmostSameAs[F]
    multipleTopmostSameAsOnInternalDS <- MultipleTopmostSameAsOnInternalDS[F]
    multipleTopmostDerivedFromOnly    <- MultipleTopmostDerivedFromOnly[F]
    migrations <- validateNames(
                    reProvisioning,
                    malformedActivityIds,
                    multiplePersonNames,
                    multipleModifiedDSData,
                    malformedDSImageIds,
                    multipleDSTopmostSameAs,
                    multipleAllWrongTopmostSameAs,
                    multipleTopmostSameAsOnInternalDS,
                    multipleTopmostDerivedFromOnly
                  )
  } yield migrations

  private[migrations] def validateNames[F[_]: MonadThrow: Logger](migrations: Migration[F]*): F[List[Migration[F]]] = {
    val groupedByName = migrations.groupBy(_.name)
    val problematicMigrations = groupedByName.collect {
      case (name, ms) if ms.size > 1 => name
    }
    if (problematicMigrations.nonEmpty) {
      val error =
        show"$categoryName: there are multiple migrations with the same name: ${problematicMigrations.mkString("; ")}"
      Logger[F]
        .error(error) >> new Exception(error).raiseError[F, List[Migration[F]]].map(_ => List.empty[Migration[F]])
    } else migrations.toList.pure[F]
  }
}

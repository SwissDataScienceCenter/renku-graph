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

package io.renku.triplesgenerator.reprovisioning

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.{CliVersion, RenkuBaseUrl, RenkuVersionPair, SchemaVersion}
import io.renku.http.client.ServiceHealthChecker
import io.renku.microservices.MicroserviceUrlFinder
import io.renku.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait ReProvisionJudge[F[_]] {
  def reProvisioningNeeded(): F[Boolean]
}

private object ReProvisionJudge {
  def apply[F[_]: Async: Logger](rdfStoreConfig: RdfStoreConfig,
                                 reProvisioningStatus:      ReProvisioningStatus[F],
                                 microserviceUrlFinder:     MicroserviceUrlFinder[F],
                                 versionCompatibilityPairs: NonEmptyList[RenkuVersionPair],
                                 timeRecorder:              SparqlQueryTimeRecorder[F]
  )(implicit renkuBaseUrl:                                  RenkuBaseUrl) = for {
    renkuVersionPairFinder <- RenkuVersionPairFinder(rdfStoreConfig, timeRecorder)
    serviceHealthChecker   <- ServiceHealthChecker[F]
  } yield new ReProvisionJudgeImpl[F](renkuVersionPairFinder,
                                      reProvisioningStatus,
                                      microserviceUrlFinder,
                                      serviceHealthChecker,
                                      versionCompatibilityPairs
  )
}

private class ReProvisionJudgeImpl[F[_]: MonadThrow](renkuVersionPairFinder: RenkuVersionPairFinder[F],
                                                     reProvisioningStatus:      ReProvisioningStatus[F],
                                                     microserviceUrlFinder:     MicroserviceUrlFinder[F],
                                                     serviceHealthChecker:      ServiceHealthChecker[F],
                                                     versionCompatibilityPairs: NonEmptyList[RenkuVersionPair]
) extends ReProvisionJudge[F] {

  import serviceHealthChecker._

  override def reProvisioningNeeded(): F[Boolean] =
    renkuVersionPairFinder.find() map decide >>= checkForZombieReProvisioning

  private def decide(maybeCurrentVersionPair: Option[RenkuVersionPair]): Boolean =
    `is current schema version different from latest`(
      maybeCurrentVersionPair.map(_.schemaVersion)
    ) || `are latest schema versions same but cli versions different`(maybeCurrentVersionPair.map(_.cliVersion))

  private def `is current schema version different from latest`(maybeCurrent: Option[SchemaVersion]) =
    !(maybeCurrent contains versionCompatibilityPairs.head.schemaVersion)

  private def `are latest schema versions same but cli versions different`(maybeCurrentCliVersion: Option[CliVersion]) =
    versionCompatibilityPairs.toList match {
      case RenkuVersionPair(latestCliVersion, latestSchemaVersion) :: RenkuVersionPair(_, oldSchemaVersion) :: _
          if latestSchemaVersion == oldSchemaVersion =>
        !(maybeCurrentCliVersion contains latestCliVersion)
      case _ => false
    }

  private lazy val checkForZombieReProvisioning: Boolean => F[Boolean] = {
    case true => true.pure[F]
    case false =>
      reProvisioningStatus.underReProvisioning() >>= {
        case false => false.pure[F]
        case true =>
          reProvisioningStatus.findReProvisioningService() >>= {
            case None => true.pure[F]
            case Some(controllerUrl) =>
              microserviceUrlFinder.findBaseUrl() >>= {
                case `controllerUrl` => true.pure[F]
                case _               => ping(controllerUrl).map(!_)
              }
          }
      }
  }
}

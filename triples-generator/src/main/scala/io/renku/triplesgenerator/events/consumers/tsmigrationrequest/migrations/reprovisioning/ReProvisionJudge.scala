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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations.reprovisioning

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, Show}
import io.renku.graph.model._
import io.renku.http.client.ServiceHealthChecker
import io.renku.microservices.MicroserviceUrlFinder
import io.renku.triplesstore.{MigrationsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

private trait ReProvisionJudge[F[_]] {
  def reProvisioningNeeded(): F[Boolean]
}

private object ReProvisionJudge {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: MigrationsConnectionConfig,
                                                          reProvisioningStatus:      ReProvisioningStatus[F],
                                                          microserviceUrlFinder:     MicroserviceUrlFinder[F],
                                                          versionCompatibilityPairs: NonEmptyList[RenkuVersionPair]
  )(implicit renkuUrl:                                                               RenkuUrl) = for {
    renkuVersionPairFinder <- RenkuVersionPairFinder(storeConfig)
    serviceHealthChecker   <- ServiceHealthChecker[F]
  } yield new ReProvisionJudgeImpl[F](renkuVersionPairFinder,
                                      reProvisioningStatus,
                                      microserviceUrlFinder,
                                      serviceHealthChecker,
                                      versionCompatibilityPairs
  )
}

private class ReProvisionJudgeImpl[F[_]: MonadThrow: Logger](renkuVersionPairFinder: RenkuVersionPairFinder[F],
                                                             reProvisioningStatus:      ReProvisioningStatus[F],
                                                             microserviceUrlFinder:     MicroserviceUrlFinder[F],
                                                             serviceHealthChecker:      ServiceHealthChecker[F],
                                                             versionCompatibilityPairs: NonEmptyList[RenkuVersionPair]
) extends ReProvisionJudge[F] {

  import serviceHealthChecker._

  override def reProvisioningNeeded(): F[Boolean] =
    (renkuVersionPairFinder.find() flatTap logVersions map decide) >>= checkForZombieReProvisioning

  private lazy val decide: Option[RenkuVersionPair] => Boolean = {
    case None => true
    case Some(tsVersionPair) =>
      `is TS schema version different from latest`(tsVersionPair.schemaVersion) ||
      `are latest schema versions same but CLI versions different`(tsVersionPair.cliVersion)
  }

  private lazy val `is TS schema version different from latest`: SchemaVersion => Boolean =
    _ != versionCompatibilityPairs.head.schemaVersion

  private lazy val `are latest schema versions same but CLI versions different`: CliVersion => Boolean = tsCliVersion =>
    versionCompatibilityPairs.toList match {
      case RenkuVersionPair(latestCliVersion, latestSchemaVersion) :: RenkuVersionPair(_, oldSchemaVersion) :: _
          if latestSchemaVersion == oldSchemaVersion =>
        tsCliVersion != latestCliVersion
      case _ => false
    }

  private def logVersions(maybeTSVersionPair: Option[RenkuVersionPair]) = {

    implicit val show: Show[(CliVersion, SchemaVersion)] = Show.show { case (cli, schema) =>
      show"schema version $schema and CLI version $cli"
    }

    val expectedVersion = versionCompatibilityPairs.toList match {
      case RenkuVersionPair(cliVersion, schemaVersion) :: _ => (cliVersion -> schemaVersion).show
      case _                                                => "unknown"
    }

    maybeTSVersionPair match {
      case Some(RenkuVersionPair(cliVersion, schemaVersion)) =>
        Logger[F].info(formMessage(show"triples Store on ${cliVersion -> schemaVersion}; expected $expectedVersion"))
      case _ =>
        Logger[F].info(formMessage(show"triples Store on unknown Schema and CLI version; expected $expectedVersion"))
    }
  }

  private lazy val checkForZombieReProvisioning: Boolean => F[Boolean] = {
    case true => true.pure[F]
    case false =>
      reProvisioningStatus.underReProvisioning() >>= {
        case false => false.pure[F]
        case true =>
          reProvisioningStatus.findReProvisioningService() >>= {
            case None =>
              Logger[F].info(formMessage("no info about service controlling re-provisioning")) >> true.pure[F]
            case Some(controllerUrl) =>
              microserviceUrlFinder.findBaseUrl() >>= {
                case `controllerUrl` =>
                  Logger[F].info(formMessage("started by this service did not finish")) >> true.pure[F]
                case _ =>
                  ping(controllerUrl).map(!_) flatTap {
                    case true  => Logger[F].info(formMessage(show"was started by $controllerUrl which is down"))
                    case false => Logger[F].info(formMessage(show"already running on $controllerUrl"))
                  }
              }
          }
      }
  }
}

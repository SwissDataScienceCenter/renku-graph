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

package io.renku.triplesgenerator.config

import cats.{ApplicativeError, MonadError, Show}
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.graph.model.versions.{CliVersion, RenkuVersionPair, SchemaVersion}
import org.typelevel.log4cats.Logger

final case class VersionCompatibilityConfig(
    configuredCliVersion: CliVersion,
    renkuDevVersion:      Option[RenkuPythonDevVersion],
    schemaVersion:        SchemaVersion,
    reProvisioningNeeded: Boolean
) {
  val cliVersion: CliVersion = renkuDevVersion.map(rd => CliVersion(rd.version)).getOrElse(configuredCliVersion)

  val asVersionPair: RenkuVersionPair = RenkuVersionPair(cliVersion, schemaVersion)
}

object VersionCompatibilityConfig {

  implicit val show: Show[VersionCompatibilityConfig] =
    Show.show(c =>
      s"[cli-version: ${c.cliVersion}, schema-version: ${c.schemaVersion}, reProvisioningNeeded=${c.reProvisioningNeeded}]"
    )

  def fromConfig(config: Config = ConfigFactory.load()): Either[Throwable, VersionCompatibilityConfig] =
    Either.catchNonFatal {
      val cfg      = config.getConfig("compatibility")
      val renkuDev = RenkuPythonDevVersionConfig[Either[Throwable, *]](config).getOrElse(None)
      VersionCompatibilityConfig(
        CliVersion(cfg.getString("cli-version")),
        renkuDev,
        SchemaVersion(cfg.getString("schema-version")),
        cfg.getBoolean("re-provisioning-needed")
      )
    }

  def fromConfigF[F[_]](
      config: Config
  )(implicit F: MonadError[F, Throwable], L: Logger[F]): F[VersionCompatibilityConfig] =
    ApplicativeError[F, Throwable]
      .fromEither(fromConfig(config))
      .flatTap(_.renkuDevVersion match {
        case Some(devVersion) =>
          L.warn(
            s"RENKU_PYTHON_DEV_VERSION env variable is set. CLI config version is now set to ${devVersion.version}"
          )
        case None => ().pure[F]
      })

  def apply(
      pair:                 RenkuVersionPair,
      reProvisioningNeeded: Boolean,
      renkuDevVersion:      Option[RenkuPythonDevVersion] = None
  ): VersionCompatibilityConfig =
    VersionCompatibilityConfig(pair.cliVersion, renkuDevVersion, pair.schemaVersion, reProvisioningNeeded)
}

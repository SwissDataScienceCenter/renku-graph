/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.init

import cats.effect.Async
import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import com.typesafe.config.Config
import io.renku.graph.model.versions.CliVersion
import io.renku.triplesgenerator.config.TriplesGeneration.{RemoteTriplesGeneration, RenkuLog}
import io.renku.triplesgenerator.config.{RenkuPythonDevVersion, RenkuPythonDevVersionConfig, TriplesGeneration, VersionCompatibilityConfig}
import org.typelevel.log4cats.Logger

trait CliVersionCompatibilityVerifier[F[_]] {
  def run: F[Unit]
}

private class CliVersionCompatibilityVerifierImpl[F[_]: MonadThrow: Logger](
    cliVersion:           CliVersion,
    compatibility:        VersionCompatibilityConfig,
    maybeRenkuDevVersion: Option[RenkuPythonDevVersion]
) extends CliVersionCompatibilityVerifier[F] {

  private val applicative: Applicative[F] = Applicative[F]
  import applicative.whenA

  override def run: F[Unit] = maybeRenkuDevVersion match {
    case Some(version) =>
      Logger[F].warn(show"Service running dev version of CLI: $version")
    case None =>
      whenA(cliVersion != compatibility.cliVersion) {
        new IllegalStateException(
          show"Incompatible versions. cliVersion: $cliVersion, configured version: ${compatibility.cliVersion}"
        ).raiseError[F, Unit]
      }
  }
}

object CliVersionCompatibilityChecker {

  // the concept of TriplesGeneration flag is a temporary solution
  // to provide acceptance-tests with the expected CLI version
  def apply[F[_]: Async: Logger](config: Config): F[CliVersionCompatibilityVerifier[F]] = for {
    compatConfig    <- VersionCompatibilityConfig.fromConfigF(config)
    renkuDevVersion <- RenkuPythonDevVersionConfig[F](config)
    cliVersion <- TriplesGeneration[F](config) >>= {
                    case RemoteTriplesGeneration =>
                      compatConfig.cliVersion.pure[F]
                    case RenkuLog =>
                      renkuDevVersion.map(v => CliVersion(v.version).pure[F]).getOrElse(CliVersionLoader[F]())
                  }
  } yield new CliVersionCompatibilityVerifierImpl[F](cliVersion, compatConfig, renkuDevVersion)
}

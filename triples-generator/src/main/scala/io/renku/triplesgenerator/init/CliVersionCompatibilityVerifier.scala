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

package io.renku.triplesgenerator.init

import cats.MonadError
import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._
import io.renku.graph.model.{CliVersion, RenkuVersionPair}
import io.renku.triplesgenerator.config.TriplesGeneration
import io.renku.triplesgenerator.config.TriplesGeneration.{RemoteTriplesGeneration, RenkuLog}

trait CliVersionCompatibilityVerifier[F[_]] {
  def run(): F[Unit]
}

private class CliVersionCompatibilityVerifierImpl[F[_]](cliVersion:        CliVersion,
                                                        renkuVersionPairs: NonEmptyList[RenkuVersionPair]
)(implicit ME:                                                             MonadError[F, Throwable])
    extends CliVersionCompatibilityVerifier[F] {
  override def run(): F[Unit] =
    if (cliVersion != renkuVersionPairs.head.cliVersion)
      ME.raiseError(
        new IllegalStateException(
          s"Incompatible versions. cliVersion: $cliVersion versionPairs: ${renkuVersionPairs.head.cliVersion}"
        )
      )
    else ().pure[F]
}

object CliVersionCompatibilityChecker {

  def apply[F[_]: Async](triplesGeneration: TriplesGeneration,
                         renkuVersionPairs: NonEmptyList[RenkuVersionPair]
  ): F[CliVersionCompatibilityVerifier[F]] = {
    // the concept of TriplesGeneration flag is a temporary solution
    // to provide acceptance-tests with the expected CLI version
    triplesGeneration match {
      case RenkuLog                => CliVersionLoader[F]()
      case RemoteTriplesGeneration => renkuVersionPairs.head.cliVersion.pure[F]
    }
  }.map(cliVersion => new CliVersionCompatibilityVerifierImpl[F](cliVersion, renkuVersionPairs))
}

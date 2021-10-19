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

package io.renku.triplesgenerator.init

import cats.MonadError
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import io.renku.graph.model.{CliVersion, RenkuVersionPair}
import io.renku.triplesgenerator.config.TriplesGeneration
import io.renku.triplesgenerator.config.TriplesGeneration.{RemoteTriplesGeneration, RenkuLog}

trait CliVersionCompatibilityVerifier[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class CliVersionCompatibilityVerifierImpl[Interpretation[_]](cliVersion:        CliVersion,
                                                                     renkuVersionPairs: NonEmptyList[RenkuVersionPair]
)(implicit ME: MonadError[Interpretation, Throwable])
    extends CliVersionCompatibilityVerifier[Interpretation] {
  override def run(): Interpretation[Unit] =
    if (cliVersion != renkuVersionPairs.head.cliVersion)
      ME.raiseError(
        new IllegalStateException(
          s"Incompatible versions. cliVersion: $cliVersion versionPairs: ${renkuVersionPairs.head.cliVersion}"
        )
      )
    else ().pure[Interpretation]
}

object IOCliVersionCompatibilityChecker {

  def apply(triplesGeneration: TriplesGeneration,
            renkuVersionPairs: NonEmptyList[RenkuVersionPair]
  ): IO[CliVersionCompatibilityVerifier[IO]] = {
    // the concept of TriplesGeneration flag is a temporary solution
    // to provide acceptance-tests with the expected CLI version
    triplesGeneration match {
      case RenkuLog                => CliVersionLoader[IO]()
      case RemoteTriplesGeneration => renkuVersionPairs.head.cliVersion.pure[IO]
    }
  }.map(cliVersion => new CliVersionCompatibilityVerifierImpl[IO](cliVersion, renkuVersionPairs))
}

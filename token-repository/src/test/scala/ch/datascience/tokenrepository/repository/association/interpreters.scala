/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository.repository.association

import cats.effect.{Bracket, IO}
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.deletion.TokenRemover
import ch.datascience.tokenrepository.repository.{AccessTokenCrypto, ProjectsTokensDB}
import io.chrisdavenport.log4cats.Logger

import scala.util.Try

private class TryAssociationPersister(
    transactor:       DbTransactor[Try, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:        Bracket[Try, Throwable])
    extends AssociationPersister[Try](transactor, queriesExecTimes)

private class IOTokenAssociator(
    pathFinder:           ProjectPathFinder[IO],
    accessTokenCrypto:    AccessTokenCrypto[IO],
    associationPersister: AssociationPersister[IO],
    tokenRemover:         TokenRemover[IO]
) extends TokenAssociator[IO](pathFinder, accessTokenCrypto, associationPersister, tokenRemover)

class IOAssociateTokenEndpoint(tokenAssociator: TokenAssociator[IO], logger: Logger[IO])
    extends AssociateTokenEndpoint[IO](tokenAssociator, logger)

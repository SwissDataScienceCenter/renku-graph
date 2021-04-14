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

package ch.datascience.tokenrepository.repository.fetching

import cats.effect.{Bracket, IO}
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.{AccessTokenCrypto, ProjectsTokensDB}
import org.typelevel.log4cats.Logger

import scala.util.Try

private class TryPersistedTokensFinder(
    transactor:       DbTransactor[Try, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:        Bracket[Try, Throwable])
    extends PersistedTokensFinder[Try](transactor, queriesExecTimes)

private class IOTokenFinder(
    tokenInRepoFinder: PersistedTokensFinder[IO],
    accessTokenCrypto: AccessTokenCrypto[IO]
) extends TokenFinder[IO](tokenInRepoFinder, accessTokenCrypto)

class IOFetchTokenEndpoint(tokenFinder: TokenFinder[IO], logger: Logger[IO])
    extends FetchTokenEndpoint[IO](tokenFinder, logger)

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

package ch.datascience.tokenrepository.repository.deletion

import cats.effect._
import ch.datascience.db.{SessionResource, SqlQuery}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.ProjectsTokensDB
import io.chrisdavenport.log4cats.Logger

import scala.util.Try

class IOTokenRemover(
    transactor:       SessionResource[IO, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
) extends TokenRemover[IO](transactor, queriesExecTimes)

class IODeleteTokenEndpoint(tokenRemover: TokenRemover[IO], logger: Logger[IO])
    extends DeleteTokenEndpoint[IO](tokenRemover, logger)

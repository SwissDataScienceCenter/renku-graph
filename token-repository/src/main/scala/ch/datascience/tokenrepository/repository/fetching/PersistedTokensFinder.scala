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

import cats.data.OptionT
import cats.effect.{Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.ProjectsTokensDB
import eu.timepit.refined.auto._

private class PersistedTokensFinder[Interpretation[_]](
    transactor:       DbTransactor[Interpretation, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:        Bracket[Interpretation, Throwable])
    extends DbClient(Some(queriesExecTimes)) {

  import doobie.implicits._

  def findToken(projectId: Id): OptionT[Interpretation, EncryptedAccessToken] = run {
    SqlQuery(
      sql"select token from projects_tokens where project_id = ${projectId.value}"
        .query[String]
        .option,
      name = "find token - id"
    )
  }

  def findToken(projectPath: Path): OptionT[Interpretation, EncryptedAccessToken] = run {
    SqlQuery(
      sql"select token from projects_tokens where project_path = ${projectPath.value}"
        .query[String]
        .option,
      name = "find token - path"
    )
  }

  private def run(query: SqlQuery[Option[String]]) = OptionT {
    measureExecutionTime(query)
      .transact(transactor.get)
      .flatMap(toSerializedAccessToken)
  }

  private lazy val toSerializedAccessToken: Option[String] => Interpretation[Option[EncryptedAccessToken]] = {
    case None => ME.pure(None)
    case Some(encryptedToken) =>
      ME.fromEither {
        EncryptedAccessToken.from(encryptedToken).map(Option.apply)
      }
  }
}

private class IOPersistedTokensFinder(
    transactor:          DbTransactor[IO, ProjectsTokensDB],
    queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
)(implicit contextShift: ContextShift[IO])
    extends PersistedTokensFinder[IO](transactor, queriesExecTimes)

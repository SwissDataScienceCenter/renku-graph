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

import cats.{Applicative, FlatMap, Monad}
import cats.data.{Kleisli, OptionT}
import cats.effect._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.ProjectsTokensDB
import eu.timepit.refined.auto._
import cats.effect._
import cats.implicits._
import skunk._
import skunk.implicits._
import skunk.codec.all._

private class PersistedTokensFinder[Interpretation[_]: Async: Bracket[*[_], Throwable]: FlatMap](
    sessionResource:  SessionResource[Interpretation, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name]
) extends DbClient[Interpretation](Some(queriesExecTimes)) {

  def findToken(projectId: Id): OptionT[Interpretation, EncryptedAccessToken] = run {
    val query: Query[Void, String] = sql"""select token from projects_tokens where project_id =""".query(varchar)
    SqlQuery[Interpretation, Option[String]](
      query = Kleisli(session => session.option(query)),
      name = "find token - id"
    )
  }

  def findToken(projectPath: Path): OptionT[Interpretation, EncryptedAccessToken] = run {
    val query: Query[Void, String] =
      sql"select token from projects_tokens where project_path = #${projectPath.value}".query(varchar)
    SqlQuery[Interpretation, Option[String]](
      Kleisli(session => session.option(query)),
      name = "find token - path"
    )
  }

  private def run(query: SqlQuery[Interpretation, Option[String]]) =
    OptionT {
      sessionResource.use { session =>
        measureExecutionTime(query, session).flatMap(toSerializedAccessToken)
      }
    }

  private lazy val toSerializedAccessToken: Option[String] => Interpretation[Option[EncryptedAccessToken]] = {
    case None => Bracket[Interpretation, Throwable].pure(None)
    case Some(encryptedToken) =>
      Bracket[Interpretation, Throwable].fromEither {
        EncryptedAccessToken.from(encryptedToken).map(Option.apply)
      }
  }
}

private class IOPersistedTokensFinder(
    transactor:       SessionResource[IO, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
) extends PersistedTokensFinder[IO](transactor, queriesExecTimes)

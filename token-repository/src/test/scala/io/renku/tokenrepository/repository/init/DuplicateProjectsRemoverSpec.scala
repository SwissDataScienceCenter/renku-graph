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

package io.renku.tokenrepository.repository.init

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.{GitLabId, Slug}
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.ProjectsTokensDB
import io.renku.tokenrepository.repository.RepositoryGenerators.encryptedAccessTokens
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import skunk._
import skunk.codec.all.{int4, varchar}
import skunk.implicits._

class DuplicateProjectsRemoverSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with DbInitSpec
    with should.Matchers
    with AsyncMockFactory {

  protected override lazy val runMigrationsUpTo: Class[_ <: DBMigration[IO]] =
    classOf[DuplicateProjectsRemover[IO]]

  it should "de-duplicate rows with the same project_path but different ids" in testDBResource.use { implicit cfg =>
    val projectSlug     = projectSlugs.generateOne
    val projectId1      = projects.GitLabId(1)
    val encryptedToken1 = encryptedAccessTokens.generateOne

    for {
      _ <- insert(projectId1, projectSlug, encryptedToken1).assertNoException

      projectId2      = projects.GitLabId(2)
      encryptedToken2 = encryptedAccessTokens.generateOne
      _ <- insert(projectId2, projectSlug, encryptedToken2).assertNoException

      projectSlug3    = projectSlugs.generateOne
      encryptedToken3 = encryptedAccessTokens.generateOne
      _ <- insert(projects.GitLabId(3), projectSlug3, encryptedToken3).assertNoException

      _ <- findToken(projectId1).asserting(_ shouldBe Some(encryptedToken1.value))
      _ <- findToken(projectId2).asserting(_ shouldBe Some(encryptedToken2.value))

      _ <- DuplicateProjectsRemover[IO].run.assertNoException
      _ <- findToken(projectId1).asserting(_ shouldBe None)
      _ <- findToken(projectId2).asserting(_ shouldBe Some(encryptedToken2.value))
      _ <- findToken(projectSlug).asserting(_ shouldBe Some(encryptedToken2.value))
      _ <- findToken(projectSlug3).asserting(_ shouldBe Some(encryptedToken3.value))
      _ <- logger.loggedOnlyF(Info("Projects de-duplicated"))
    } yield Succeeded
  }

  protected def insert(projectId: GitLabId, projectSlug: Slug, encryptedToken: EncryptedAccessToken)(implicit
      cfg: DBConfig[ProjectsTokensDB]
  ): IO[Unit] =
    sessionResource(cfg).use { session =>
      val query: Command[Int *: String *: String *: EmptyTuple] =
        sql"""insert into
              projects_tokens (project_id, project_path, token)
              values ($int4, $varchar, $varchar)
         """.command
      session
        .prepare(query)
        .flatMap(_.execute(projectId.value *: projectSlug.value *: encryptedToken.value *: EmptyTuple))
        .map(assureInserted)
    }
}

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

package ch.datascience.tokenrepository.repository.init

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import ch.datascience.metrics.TestLabeledHistogram
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.RepositoryGenerators.encryptedAccessTokens
import ch.datascience.tokenrepository.repository.association.ProjectPathFinder
import ch.datascience.tokenrepository.repository.deletion.TokenRemover
import ch.datascience.tokenrepository.repository.{IOAccessTokenCrypto, InMemoryProjectsTokensDbSpec}
import eu.timepit.refined.auto._
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

class ProjectPathAdderSpec
    extends AnyWordSpec
    with InMemoryProjectsTokensDbSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  "run" should {

    "do nothing if the 'project_path' column already exists" in new TestCase {
      if (!tableExists()) createTable()
      addProjectPath()
      checkColumnExists shouldBe true

      projectPathAdder.run().unsafeRunSync() shouldBe ((): Unit)

      checkColumnExists shouldBe true

      logger.loggedOnly(Info("'project_path' column exists"))
    }

    "add the 'project_path' column if does not exist and add paths fetched from GitLab" in new TestCase {
      if (tableExists()) {
        dropTable()
        createTable()
      }
      checkColumnExists shouldBe false

      val project1Id             = projectIds.generateOne
      val project1Path           = projectPaths.generateOne
      val project1TokenEncrypted = encryptedAccessTokens.generateOne
      val project1Token          = accessTokens.generateOne
      insert(project1Id, project1TokenEncrypted)
      assumePathExistsInGitLab(project1Id, Some(project1Path), project1TokenEncrypted, project1Token)

      val project2Id             = projectIds.generateOne
      val project2Path           = projectPaths.generateOne
      val project2TokenEncrypted = encryptedAccessTokens.generateOne
      val project2Token          = accessTokens.generateOne
      insert(project2Id, project2TokenEncrypted)
      assumePathExistsInGitLab(project2Id, Some(project2Path), project2TokenEncrypted, project2Token)

      projectPathAdder.run().unsafeRunSync() shouldBe ((): Unit)

      eventually {
        findToken(project1Path) shouldBe Some(project1TokenEncrypted.value)
        findToken(project2Path) shouldBe Some(project2TokenEncrypted.value)
      }

      eventually {
        logger.loggedOnly(Info("'project_path' column added"))
      }
      eventually {
        verifyTrue(sql"DROP INDEX idx_project_path;".command)
      }
    }

    "add the 'project_path' column if does not exist and remove entries for non-existing projects in GitLab" in new TestCase {
      if (tableExists()) {
        dropTable()
        createTable()
      }
      checkColumnExists shouldBe false

      val project1Id             = projectIds.generateOne
      val project1TokenEncrypted = encryptedAccessTokens.generateOne
      val project1Token          = accessTokens.generateOne
      insert(project1Id, project1TokenEncrypted)
      assumePathExistsInGitLab(project1Id, None, project1TokenEncrypted, project1Token)

      val project2Id             = projectIds.generateOne
      val project2Path           = projectPaths.generateOne
      val project2TokenEncrypted = encryptedAccessTokens.generateOne
      val project2Token          = accessTokens.generateOne
      insert(project2Id, project2TokenEncrypted)
      assumePathExistsInGitLab(project2Id, Some(project2Path), project2TokenEncrypted, project2Token)

      projectPathAdder.run().unsafeRunSync() shouldBe ((): Unit)

      eventually {
        findToken(project2Path) shouldBe Some(project2TokenEncrypted.value)
      }

      eventually {
        verifyTrue(sql"DROP INDEX idx_project_path;".command)
      }

      eventually {
        logger.loggedOnly(Info("'project_path' column added"))
      }
    }
  }

  private trait TestCase {
    val logger            = TestLogger[IO]()
    val accessTokenCrypto = mock[IOAccessTokenCrypto]
    val pathFinder        = mock[ProjectPathFinder[IO]]
    val queriesExecTimes  = TestLabeledHistogram[SqlQuery.Name]("query_id")
    val tokenRemover      = new TokenRemover[IO](sessionResource, queriesExecTimes)
    val projectPathAdder =
      new ProjectPathAdderImpl[IO](sessionResource, accessTokenCrypto, pathFinder, tokenRemover, logger)

    def assumePathExistsInGitLab(projectId:        Id,
                                 maybeProjectPath: Option[Path],
                                 encryptedToken:   EncryptedAccessToken,
                                 token:            AccessToken
    ) = {
      (accessTokenCrypto.decrypt _)
        .expects(encryptedToken)
        .returning(token.pure[IO])
        .atLeastOnce()
      (pathFinder
        .findProjectPath(_: Id, _: Option[AccessToken]))
        .expects(projectId, Some(token))
        .returning(maybeProjectPath.pure[IO])
        .atLeastOnce()
    }
  }

  private def addProjectPath(): Unit = execute {
    Kleisli[IO, Session[IO], Unit] { session =>
      val query: Command[Void] =
        sql"""ALTER TABLE projects_tokens
              ADD COLUMN project_path VARCHAR;
        """.command
      session.execute(query).void
    }
  }

  private def checkColumnExists: Boolean = sessionResource
    .useK {
      val query: Query[Void, Path] = sql"select project_path from projects_tokens limit 1"
        .query(varchar)
        .gmap[Path]
      Kleisli {
        _.option(query)
          .map(_ => true)
          .recover { case _ => false }
      }
    }
    .unsafeRunSync()

  def insert(projectId: Id, encryptedToken: EncryptedAccessToken): Unit = execute {
    Kleisli[IO, Session[IO], Unit] { session =>
      val query: Command[Int ~ String] =
        sql"""insert into
            projects_tokens (project_id, token)
            values ($int4, $varchar)
         """.command
      session.prepare(query).use(_.execute(projectId.value ~ encryptedToken.value)).map(assureInserted)
    }
  }

  private lazy val assureInserted: Completion => Unit = {
    case Completion.Insert(1) => ()
    case _                    => fail("insertion problem")
  }

  protected override def createTable(): Unit = execute {
    Kleisli[IO, Session[IO], Unit] { session =>
      val query: Command[Void] =
        sql"""CREATE TABLE projects_tokens(
                project_id int4 PRIMARY KEY,
                token VARCHAR NOT NULL
              );
        """.command
      session.execute(query).void
    }
  }
}

/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookcreation

import cats.Id
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators.projectIds
import ch.datascience.graph.events.ProjectId
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators.gitLabAuthTokens
import ch.datascience.webhookservice.model.GitLabAuthToken
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.language.higherKinds
import scala.util.Try

class HookCreationSpec extends WordSpec with MockFactory {

//  "create" should {
//
//    "log success and return right if creation of the hook in GitLab was successful" in new HappyTestCase {
//
//      (gitLabHookCreation.createHook(_: ProjectId, _: GitLabAuthToken))
//        .expects(projectId, authToken)
//
//      hookCreation.createHook(projectId, authToken) shouldBe ((): Unit)
//
//      logger.loggedOnly(Info, s"Hook created for project with id $projectId")
//    }
//
//    "log an error and return left if creation of the hook in GitLab was unsuccessful" in new ErrorTestCase {
//
//      val error = context.raiseError(exceptions.generateOne)
//      (gitLabHookCreation.createHook(_: ProjectId, _: GitLabAuthToken))
//        .expects(projectId, authToken)
//        .returning(error)
//
//      hookCreation.createHook(projectId, authToken) shouldBe error
//
//      logger.expectNoLogs()
//    }
//  }
//
//  private trait TestCase {
//
//    val projectId = projectIds.generateOne
//    val authToken = gitLabAuthTokens.generateOne
//
//  }
//
//  private trait HappyTestCase extends TestCase {
//
//    val logger = TestLogger[Id]()
//    val gitLabHookCreation = mock[GitLabHookCreation[Id]]
//    val hookCreation = new HookCreation[Id](gitLabHookCreation, logger)
//  }
//
//  private trait ErrorTestCase extends TestCase {
//
//    import cats._
//    import cats.implicits._
//
//    val context = MonadError[Try, Throwable]
//
//    val logger = TestLogger[Try]()
//    val gitLabHookCreation = mock[GitLabHookCreation[Try]]
//    val hookCreation = new HookCreation[Try](gitLabHookCreation, logger)
//  }
}

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

package io.renku.graph.acceptancetests.stubs.gitlab

import cats.effect.IO
import io.renku.graph.acceptancetests.tooling.BaseSpec
import io.renku.graph.model.{GitLabApiUrl, GitLabUrl}
import org.scalatest._

/** Mixin for tests that starts a [[GitLabApiStub]] for the whole suite and clears its state before each test. */
trait GitLabStubSupport extends BeforeAndAfterAll with BeforeAndAfter { self: BaseSpec =>

  implicit val gitLabUrl:    GitLabUrl    = GitLabUrl(testConfig.getString(GitLabStubSupport.gitLabUrlKey))
  implicit val gitLabApiUrl: GitLabApiUrl = gitLabUrl.apiV4

  lazy val gitLabStub: GitLabApiStub[IO] = GitLabApiStub.empty[IO].unsafeRunSync()

  private var gitLabStubShutdown: IO[Unit] = IO.pure(())

  override def beforeAll(): Unit = {
    super.beforeAll()
    val uri           = configUri(GitLabStubSupport.gitLabUrlKey)
    val host          = uri.host.getOrElse(sys.error("No gitlab host found"))
    val port          = uri.port.getOrElse(sys.error("No gitlab port found"))
    val (_, shutdown) = gitLabStub.resource(host.value, port).allocated.unsafeRunSync()
    this.gitLabStubShutdown = shutdown
  }

  override def afterAll(): Unit = {
    super.afterAll()
    testLogger.info("Shutting down GitLabApiStub").unsafeRunSync()
    gitLabStubShutdown.unsafeRunSync()
  }

  before {
    gitLabStub.update(GitLabStateUpdates.clearState).unsafeRunSync()
  }
}

object GitLabStubSupport {
  private val gitLabUrlKey = "services.gitlab.url"
}

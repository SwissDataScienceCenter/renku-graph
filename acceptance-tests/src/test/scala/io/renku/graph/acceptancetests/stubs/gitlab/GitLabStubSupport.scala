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

package io.renku.graph.acceptancetests.stubs.gitlab

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabStubSupport.GitLabStubStarter
import io.renku.graph.acceptancetests.tooling.AcceptanceSpec
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.{GitLabApiUrl, GitLabUrl}
import org.http4s.Uri
import org.scalatest._
import org.typelevel.log4cats.Logger

import scala.util.Try

/** Mixin for tests that starts a [[GitLabApiStub]] for the whole suite and clears its state before each test. */
trait GitLabStubSupport extends BeforeAndAfterAll with BeforeAndAfter with GitLabStubIOSyntax { self: AcceptanceSpec =>

  implicit val gitLabUrl:    GitLabUrl    = GitLabUrlLoader[Try]().fold(throw _, identity)
  implicit val gitLabApiUrl: GitLabApiUrl = gitLabUrl.apiV4

  private lazy val gitLabStubStarter = GitLabStubStarter(gitLabUrl)
  lazy val gitLabStub: GitLabApiStub[IO] = gitLabStubStarter.stub

  override def beforeAll(): Unit = {
    super.beforeAll()
    gitLabStubStarter.startup()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    gitLabStubStarter.shutdown()
  }

  before {
    gitLabStub.clearState()
  }
}
object GitLabStubSupport {
  final class GitLabStubStarter(gitLabUrl: GitLabUrl, val stub: GitLabApiStub[IO])(implicit rt: IORuntime) {
    private val uri  = Uri.unsafeFromString(gitLabUrl.value)
    private val host = uri.host.getOrElse(sys.error("No gitlab host found"))
    private val port = uri.port.getOrElse(sys.error("No gitlab port found"))

    private var close: IO[Unit] = IO.pure(())

    def startup(): Unit = {
      val (_, close) = stub.resource(host.value, port).allocated.unsafeRunSync()
      this.close = close
    }

    def shutdown(): Unit =
      close.unsafeRunSync()
  }

  object GitLabStubStarter {
    def apply(gitLabUrl: GitLabUrl)(implicit rt: IORuntime, logger: Logger[IO]): GitLabStubStarter = {
      val stub = GitLabApiStub.empty[IO].unsafeRunSync()
      new GitLabStubStarter(gitLabUrl, stub)
    }
  }
}

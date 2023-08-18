/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.projectauth

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.Stream
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.Visibility
import io.renku.triplesstore.client.util.JenaContainerSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class ProjectAuthServiceSpec extends AsyncFlatSpec with AsyncIOSpec with JenaContainerSpec with should.Matchers {
  implicit val logger:   Logger[IO] = Slf4jLogger.getLogger[IO]
  implicit val renkuUrl: RenkuUrl   = RenkuUrl("http://localhost/renku")

  def withProjectAuthService: Resource[IO, ProjectAuthService[IO]] =
    withDataset("projectauth").map(ProjectAuthService[IO](_, renkuUrl))

  it should "add data" in {
    withProjectAuthService.use { s =>
      for {
        data <- Generators.projectAuthDataGen.asStream.toIO.take(20).compile.toVector
        _ <- Stream
               .emits(data)
               .through(s.updateAll)
               .compile
               .drain

        n <- s.getAll(15).compile.toVector
        _ = n shouldBe data.sortBy(_.slug)
      } yield ()
    }
  }

  it should "work with no members" in {
    withProjectAuthService.use { s =>
      for {
        data <- Generators.projectAuthDataGen.asStream.toIO
                  .take(2)
                  .map(_.copy(members = Set.empty))
                  .compile
                  .toVector
        _ <- Stream
               .emits(data)
               .through(s.updateAll)
               .compile
               .drain

        n <- s.getAll().compile.toVector
        _ = n shouldBe data.sortBy(_.slug)
      } yield ()
    }
  }

  it should "remove projects" in {
    withProjectAuthService.use { s =>
      for {
        original <- Generators.projectAuthDataGen.asStream.toIO
                      .take(1)
                      .map(_.copy(visibility = Visibility.Internal))
                      .compile
                      .lastOrError
        _ <- s.update(original)

        n <- s.getAll().compile.toVector
        _ = n.head shouldBe original

        _ <- s.remove(original.slug)
        _ <- s.getAll().compile.toVector.asserting(v => v shouldBe Vector.empty)
      } yield ()
    }
  }

  it should "remove selectively" in {
    withProjectAuthService.use { s =>
      for {
        data <- Generators.projectAuthDataGen.asStream.toIO
                  .take(6)
                  .compile
                  .toVector
        _ <- Stream.emits(data).through(s.updateAll).compile.drain

        (toremove, tokeep) = data.splitAt(3)
        _ <- s.remove(NonEmptyList.fromListUnsafe(toremove.map(_.slug).toList))

        _ <- s.getAll().compile.toVector.asserting(v => v shouldBe tokeep.sortBy(_.slug))
      } yield ()
    }
  }

  it should "update new properties" in {
    withProjectAuthService.use { s =>
      for {
        original <- Generators.projectAuthDataGen.asStream.toIO
                      .take(1)
                      .map(_.copy(visibility = Visibility.Internal))
                      .compile
                      .lastOrError
        _ <- s.update(original)

        n <- s.getAll().compile.toVector
        _ = n.head shouldBe original

        second = original.copy(visibility = Visibility.Public)
        _  <- s.update(second)
        n2 <- s.getAll().compile.toVector
        _ = n2.head shouldBe second

        third = second.copy(members = second.members + ProjectMember(GitLabId(43), Role.Reader))
        _  <- s.update(third)
        n3 <- s.getAll().compile.toVector
        _ = n3.head shouldBe third
      } yield ()
    }
  }
}

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
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.{Role, Slug, Visibility}
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class ProjectAuthServiceSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with ProjectAuthServiceSupport
    with should.Matchers {
  implicit val logger:   Logger[IO] = Slf4jLogger.getLogger[IO]
  implicit val renkuUrl: RenkuUrl   = RenkuUrl("http://localhost/renku")

  private val anyIsGood: ProjectAuthData => Boolean = _ => true

  def randomData(num: Int, suchThat: ProjectAuthData => Boolean = anyIsGood) =
    Generators.projectAuthDataGen.suchThat(suchThat).asStream.take(num)

  it should "add data" in {
    withProjectAuthServiceData(randomData(20)).use { case (s, data) =>
      for {
        n <- s.getAll(QueryFilter.all, 15).compile.toVector
        _ = n shouldBe data.sortBy(_.slug)
      } yield ()
    }
  }

  it should "work with no members" in {
    withProjectAuthServiceData(randomData(2)).use { case (s, data) =>
      for {
        n <- s.getAll(QueryFilter.all).compile.toVector
        _ = n shouldBe data.sortBy(_.slug)
      } yield ()
    }
  }

  it should "remove projects" in {
    val data = Generators.projectAuthData.withVisibility(Visibility.Internal).stream
    withProjectAuthServiceData(data.take(1)).use { case (s, original) =>
      for {
        n <- s.getAll(QueryFilter.all).compile.toList
        _ = n shouldBe original

        _ <- s.remove(original.head.slug)
        _ <- s.getAll(QueryFilter.all).compile.toVector.asserting(v => v shouldBe Vector.empty)
      } yield ()
    }
  }

  it should "remove selectively" in {
    withProjectAuthServiceData(randomData(6)).use { case (s, data) =>
      val (toremove, tokeep) = data.splitAt(3)
      for {
        _ <- s.remove(NonEmptyList.fromListUnsafe(toremove.map(_.slug).toList))
        _ <- s.getAll(QueryFilter.all).compile.toVector.asserting(v => v shouldBe tokeep.sortBy(_.slug))
      } yield ()
    }
  }

  it should "update new properties" in {
    val data = Generators.projectAuthData.withVisibility(Visibility.Internal).stream
    withProjectAuthServiceData(data.take(1)).use { case (s, original) =>
      for {
        n <- s.getAll(QueryFilter.all).compile.toList
        _ = n shouldBe original

        second = original.head.copy(visibility = Visibility.Public)
        _  <- s.update(second)
        n2 <- s.getAll(QueryFilter.all).compile.toVector
        _ = n2.head shouldBe second

        third = second.copy(members = second.members + ProjectMember(GitLabId(43), Role.Reader))
        _  <- s.update(third)
        n3 <- s.getAll(QueryFilter.all).compile.toVector
        _ = n3.head shouldBe third
      } yield ()
    }
  }

  it should "search for a specific project by slug" in {
    withProjectAuthServiceData(randomData(1)).use { case (s, original) =>
      for {
        found <- s.getAll(QueryFilter.all.withSlug(original.head.slug)).compile.lastOrError
        nf    <- s.getAll(QueryFilter.all.withSlug(Slug(original.head.slug.value + "x"))).compile.last

        _ = found shouldBe original.head
        _ = nf    shouldBe None
      } yield ()
    }
  }

  it should "search by member id" in {
    withProjectAuthServiceData(randomData(1, suchThat = _.members.nonEmpty)).use { case (s, original) =>
      for {
        found <- s.getAll(QueryFilter.all.withMember(original.head.members.head.gitLabId)).compile.lastOrError

        gId <- IO(
                 RenkuTinyTypeGenerators.personGitLabIds
                   .suchThat(id => !original.head.members.map(_.gitLabId).contains(id))
                   .generateOne
               )
        nf <- s.getAll(QueryFilter.all.withMember(gId)).compile.last

        _ = found shouldBe original.head
        _ = nf    shouldBe None
      } yield ()
    }
  }
}

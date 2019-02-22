/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing

import cats.MonadError
import cats.data.NonEmptyList
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import io.circe._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Try}

class CommitEventsDeserialiserSpec extends WordSpec {

  "deserialiseToCommitEvents" should {

    "produce a single CommitEvent if the Json string can be successfully deserialized and there are no parents" in new TestCase {
      deserialiser.deserialiseToCommitEvents(commitEvent(parents = Nil)) shouldBe context.pure(
        NonEmptyList.of(
          CommitWithoutParent(
            commitId,
            projectPath
          )
        )
      )
    }

    "produce CommitEvents for all the parents if they are present" in new TestCase {
      val parentCommits = parentsIdsLists(minNumber = 1).generateOne

      deserialiser.deserialiseToCommitEvents(commitEvent(parentCommits)) shouldBe context.pure(
        NonEmptyList.fromListUnsafe(
          parentCommits map { parentCommitId =>
            CommitWithParent(
              commitId,
              parentCommitId,
              projectPath
            )
          }
        )
      )
    }

    "fail if parsing fails" in new TestCase {
      val Failure(ParsingFailure(message, underlying)) = deserialiser.deserialiseToCommitEvents("{")

      message    shouldBe "CommitEvent cannot be deserialised: '{'"
      underlying shouldBe a[ParsingFailure]
    }

    "fail if decoding fails" in new TestCase {
      val Failure(DecodingFailure(message, _)) = deserialiser.deserialiseToCommitEvents("{}")

      message shouldBe "CommitEvent cannot be deserialised: '{}'"
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val commitId    = commitIds.generateOne
    val projectPath = projectPaths.generateOne

    val deserialiser = new CommitEventsDeserialiser[Try]

    def commitEvent(parents: List[CommitId]): String =
      Json
        .obj(
          "id"      -> Json.fromString(commitId.value),
          "parents" -> Json.arr(parents.map(_.value).map(Json.fromString): _*),
          "project" -> Json.obj(
            "path" -> Json.fromString(projectPath.value)
          )
        )
        .noSpaces
  }
}

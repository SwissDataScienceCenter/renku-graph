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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration

import cats.MonadError
import cats.data.NonEmptyList
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{CommitId, EventBody, EventId}
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.CommitEvent.{CommitEventWithParent, CommitEventWithoutParent}
import ch.datascience.triplesgenerator.events.categories.models.Project
import io.circe._
import org.scalacheck.Gen
import org.scalacheck.Gen.choose
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class EventBodyDeserialiserSpec extends AnyWordSpec with should.Matchers {

  "toCommitEvents" should {

    "produce a single CommitEvent if the Json string can be successfully deserialized and there are no parents" in new TestCase {
      deserializer.toCommitEvents(commitEvent(parents = Nil)) shouldBe context.pure(
        NonEmptyList.of(
          CommitEventWithoutParent(
            EventId(commitId.value),
            Project(projectId, projectPath),
            commitId
          )
        )
      )
    }

    "produce CommitEvents for all the parents if they are present" in new TestCase {
      val parentCommits = parentsIdsLists(minNumber = 1).generateOne

      deserializer.toCommitEvents(commitEvent(parentCommits)) shouldBe context.pure(
        NonEmptyList.fromListUnsafe(
          parentCommits map { parentCommitId =>
            CommitEventWithParent(
              EventId(commitId.value),
              Project(projectId, projectPath),
              commitId,
              parentCommitId
            )
          }
        )
      )
    }

    "fail if parsing fails" in new TestCase {
      val Failure(ParsingFailure(message, underlying)) = deserializer.toCommitEvents(EventBody("{"))

      message    shouldBe "CommitEvent cannot be deserialised: '{'"
      underlying shouldBe a[ParsingFailure]
    }

    "fail if decoding fails" in new TestCase {
      val Failure(DecodingFailure(message, _)) = deserializer.toCommitEvents(EventBody("{}"))

      message shouldBe "CommitEvent cannot be deserialised: '{}'"
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val commitId    = commitIds.generateOne
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val deserializer = new EventBodyDeserializerImpl[Try]

    def commitEvent(parents: List[CommitId]): EventBody = EventBody {
      Json
        .obj(
          "id"      -> Json.fromString(commitId.value),
          "parents" -> Json.arr(parents.map(_.value).map(Json.fromString): _*),
          "project" -> Json.obj(
            "id"   -> Json.fromInt(projectId.value),
            "path" -> Json.fromString(projectPath.value)
          )
        )
        .noSpaces
    }
  }

  private implicit def parentsIdsLists(minNumber: Int = 0, maxNumber: Int = 4): Gen[List[CommitId]] = {
    require(minNumber <= maxNumber,
            s"minNumber = $minNumber is not <= maxNumber = $maxNumber for generating parents Ids list"
    )
    for {
      parentCommitsNumber <- choose(minNumber, maxNumber)
      parents             <- Gen.listOfN(parentCommitsNumber, commitIds)
    } yield parents
  }
}

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

package io.renku.entities.searchgraphs

import Generators.updateCommands
import cats.syntax.all._
import commands.UpdateCommand
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{SparqlQuery, TSClient}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class UpdateCommandsUploaderSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "upload" should {

    "succeeds if uploading updates to the TS succeeds" in new TestCase {

      val commands = updateCommands.generateList()

      toSparqlQueries(commands) foreach { query =>
        givenTSUpdating(query, returning = ().pure[Try])
      }

      uploader.upload(commands) shouldBe ().pure[Try]
    }

    "fail if uploading updates to the TS fails" in new TestCase {

      val commands = updateCommands.generateList()

      val query1 :: query2 :: Nil = toSparqlQueries(commands)
      givenTSUpdating(query1, returning = ().pure[Try])
      val failure = exceptions.generateOne.raiseError[Try, Unit]
      givenTSUpdating(query2, returning = failure)

      uploader.upload(commands) shouldBe failure
    }
  }

  private def toSparqlQueries(commands: List[UpdateCommand]): List[SparqlQuery] =
    commands
      .groupBy {
        case _: UpdateCommand.Insert => true
        case _: UpdateCommand.Delete => false
      }
      .map {
        case (true, cmds) =>
          SparqlQuery.of("search info inserts", s"INSERT DATA {\n${cmds.map(_.quad.asSparql).combineAll.sparql}\n}")
        case (false, cmds) =>
          SparqlQuery.of("search info deletes", s"DELETE DATA {\n${cmds.map(_.quad.asSparql).combineAll.sparql}\n}")
      }
      .toList

  private trait TestCase {

    private val tsClient: TSClient[Try] = mock[TSClient[Try]]
    val uploader = new UpdateCommandsUploaderImpl[Try](tsClient)

    def givenTSUpdating(query: SparqlQuery, returning: Try[Unit]) =
      (tsClient.updateWithNoResult _)
        .expects(query)
        .returning(returning)
  }
}

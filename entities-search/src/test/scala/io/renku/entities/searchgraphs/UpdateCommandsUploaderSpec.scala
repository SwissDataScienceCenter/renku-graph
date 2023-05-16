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

import Generators.{deleteUpdateCommands, insertUpdateCommands, queryUpdateCommands}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{SparqlQuery, TSClient}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class UpdateCommandsUploaderSpec
    extends AnyWordSpec
    with should.Matchers
    with MockFactory
    with TableDrivenPropertyChecks {

  "upload" should {

    forAll {
      Table(
        "type"   -> "generator",
        "insert" -> insertUpdateCommands,
        "delete" -> deleteUpdateCommands,
        "query"  -> queryUpdateCommands
      )
    } { (typ, commandsGen) =>
      s"succeeds if uploading updates of type $typ to the TS succeeds" in new TestCase {

        val commands = commandsGen.generateList(min = 1)

        toSparqlQueries(commands) foreach { query =>
          givenTSUpdating(query, returning = ().pure[Try])
        }

        uploader.upload(commands) shouldBe ().pure[Try]
      }
    }

    "fail if uploading updates to the TS fails" in new TestCase {

      val commands = allSortsOfCommands.generateOne

      val query1 :: query2 :: query3 :: Nil = toSparqlQueries(commands)
      givenTSUpdating(query1, returning = ().pure[Try])
      givenTSUpdating(query2, returning = ().pure[Try])
      val failure = exceptions.generateOne.raiseError[Try, Unit]
      givenTSUpdating(query3, returning = failure)

      uploader.upload(commands) shouldBe failure
    }
  }

  private def toSparqlQueries(commands: List[UpdateCommand]): List[SparqlQuery] =
    commands
      .groupBy {
        case _: UpdateCommand.Insert => "insert"
        case _: UpdateCommand.Delete => "delete"
        case _: UpdateCommand.Query  => "query"
      }
      .flatMap {
        case ("insert", cmds) =>
          List(
            SparqlQuery.of(
              "search info inserts",
              sparql"INSERT DATA {\n${cmds.map { case UpdateCommand.Insert(quad) => quad.asSparql }.combineAll}\n}"
            )
          )
        case ("delete", cmds) =>
          List(
            SparqlQuery.of(
              "search info deletes",
              sparql"DELETE DATA {\n${cmds.map { case UpdateCommand.Delete(quad) => quad.asSparql }.combineAll}\n}"
            )
          )
        case ("query", cmds) =>
          cmds.map { case UpdateCommand.Query(query) => query }
        case (other, _) =>
          throw new Exception(s"UpdateCommand of type '$other' not supported by the test")
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

  private lazy val allSortsOfCommands: Gen[List[UpdateCommand]] =
    (insertUpdateCommands, deleteUpdateCommands, queryUpdateCommands)
      .mapN(_ :: _ :: _ :: Nil)
}

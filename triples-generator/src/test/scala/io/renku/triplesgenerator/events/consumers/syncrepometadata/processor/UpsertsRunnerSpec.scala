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

package io.renku.triplesgenerator.events.consumers.syncrepometadata
package processor

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, positiveInts}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.{SparqlQuery, TSClient}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class UpsertsRunnerSpec extends AsyncFlatSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "succeed if all the given queries run fine" in {

    val queries = sparqlQueries.generateList()

    queries foreach (givenQueryRunning(_, returning = ().pure[IO]))

    runner.run(queries).assertNoException
  }

  it should "succeed even if some queries fail" in {

    val queries = sparqlQueries.generateList(min = 2)

    val exception       = exceptions.generateOne
    val failingQueryIdx = positiveInts(queries.size).generateOne.value - 1
    queries.zipWithIndex foreach {
      case (query, `failingQueryIdx`) => givenQueryRunning(query, returning = exception.raiseError[IO, Unit])
      case (query, _)                 => givenQueryRunning(query, returning = ().pure[IO])
    }

    runner.run(queries).assertNoException >>
      logger
        .loggedOnly(
          Error(
            show"$categoryName: running '${queries.get(failingQueryIdx).map(_.name).getOrElse(fail(s"no query with $failingQueryIdx index")).value}'",
            exception
          )
        )
        .pure[IO]
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger()
  private lazy val tsClient = mock[TSClient[IO]]
  private lazy val runner   = new UpsertsRunnerImpl[IO](tsClient)

  private def givenQueryRunning(query: SparqlQuery, returning: IO[Unit]) =
    (tsClient.updateWithNoResult _).expects(query).returning(returning)
}

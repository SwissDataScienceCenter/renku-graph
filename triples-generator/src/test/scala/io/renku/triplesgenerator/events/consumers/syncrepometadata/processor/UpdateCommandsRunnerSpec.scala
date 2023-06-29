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

import Generators._
import cats.effect.IO
import cats.syntax.all._
import io.renku.eventlog
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.{SparqlQuery, TSClient}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class UpdateCommandsRunnerSpec extends AsyncFlatSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  it should "succeed if running the Sparql update succeeds" in {

    val update = sparqlUpdateCommands.generateOne

    givenQueryRunning(update.value, returning = ().pure[IO])

    runner.run(update).assertNoException
  }

  it should "succeed if the given Event sending succeeds" in {

    val update = eventUpdateCommands.generateOne

    givenEventSending(update.value, returning = ().pure[IO])

    runner.run(update).assertNoException
  }

  it should "succeed even if running the query fails" in {

    logger.reset()

    val update = sparqlUpdateCommands.generateOne

    val exception = exceptions.generateOne
    givenQueryRunning(update.value, returning = exception.raiseError[IO, Unit])

    runner.run(update).assertNoException >>
      logger.loggedOnly(Error(show"$categoryName: running '${update.value.name.value}' fails", exception)).pure[IO]
  }

  it should "succeed even if sending the event fails" in {

    logger.reset()

    val update = eventUpdateCommands.generateOne

    val exception = exceptions.generateOne
    givenEventSending(update.value, returning = exception.raiseError[IO, Unit])

    runner.run(update).assertNoException >>
      logger.loggedOnly(Error(show"$categoryName: sending ${update.value} fails", exception)).pure[IO]
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger()
  private lazy val tsClient = mock[TSClient[IO]]
  private lazy val elClient = mock[eventlog.api.events.Client[IO]]
  private lazy val runner   = new UpdateCommandsRunnerImpl[IO](tsClient, elClient)

  private def givenQueryRunning(query: SparqlQuery, returning: IO[Unit]) =
    (tsClient.updateWithNoResult _).expects(query).returning(returning)

  private def givenEventSending(event: StatusChangeEvent.RedoProjectTransformation, returning: IO[Unit]) =
    (elClient.send(_: StatusChangeEvent.RedoProjectTransformation)).expects(event).returning(returning)
}

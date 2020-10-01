/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplesuploading

import java.util.concurrent.TimeUnit

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.rdfstore.CypherQuery
import io.chrisdavenport.log4cats.Logger
import org.neo4j.driver.{Record, Session}

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._
import scala.util.Try

private trait GraphUpdatesUploader[Interpretation[_]] {
  def send(updateQuery: CypherQuery): Interpretation[TriplesUploadResult]
}

private class IOGraphUpdatesUploader(
    logger:            Logger[IO],
    graphTimeRecorder: ExecutionTimeRecorder[IO]
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO]
) extends GraphUpdatesUploader[IO] {

  import TriplesUploadResult._
  override def send(updateQuery: CypherQuery): IO[TriplesUploadResult] =
    graphTimeRecorder
      .measureExecutionTime(
        IO.pure {
          logger.info(s"Starting update query - ${updateQuery.name}")
          Try {
            val session: Session = Neo4jConfig.driver.session()
            logger.info(s"Session open for update query - ${updateQuery.name} ${updateQuery.toString}")
            val resultSummary = session.writeTransaction { tx =>
              val result = tx.run(updateQuery.toString)
              logger.info(s"Query ran for update query - ${updateQuery.name}")
              val summary = result.consume()
              logger.info(s"Query consumed for update query - ${updateQuery.name}")
              summary
            }
            logger.info(
              s"Update query done in ${resultSummary.resultAvailableAfter(TimeUnit.MILLISECONDS)} ms - $resultSummary"
            )
            resultSummary
          }.getOrElse(throw new Exception(s"Could not execute query: ${updateQuery.name}"))
        }.map(_ => DeliverySuccess),
        Some(updateQuery.name)
      )
      .map(graphTimeRecorder.logExecutionTime(withMessage = "Cypher update query finished"))

}

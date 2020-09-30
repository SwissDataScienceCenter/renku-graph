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

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.rdfstore.CypherQuery
import io.chrisdavenport.log4cats.Logger
import org.neo4j.driver.{Record, Session}

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

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
      .measureExecutionTime({
                              val session: Session = Neo4jConfig.driver.session()
                              IO.pure(session.run(updateQuery.toString)).map(r => (session, r))
                            },
                            Some(updateQuery.name)
      )
      .map { case (elapsedTime, (session, result)) =>
        graphTimeRecorder.logExecutionTime(withMessage = "Cypher update query finished")
        (elapsedTime, (session, result))
      }
      .map { case (elapsedTime, (session, result)) =>
        val resultString = result
          .list()
          .asScala
          .map((record: Record) => s"values: ${record.values()}")
          .mkString("\n")
        session.close()
        logger.info(s"Update query done in ${elapsedTime.value} ms - $resultString")
        result
      }
      .map(_ => DeliverySuccess)
}

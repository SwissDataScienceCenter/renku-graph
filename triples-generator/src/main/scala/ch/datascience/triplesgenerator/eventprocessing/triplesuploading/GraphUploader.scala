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

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.TriplesUploadResult.DeliverySuccess
import io.chrisdavenport.log4cats.Logger
import org.neo4j.driver.{Record, Session}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait GraphUploader[Interpretation[_]] {
  def upload(triples: JsonLDTriples): Interpretation[TriplesUploadResult]
}

private class IOGraphUploader(
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends GraphUploader[IO]
    with Neo4jConfig {

  import scala.collection.JavaConverters._
  def upload(triples: JsonLDTriples): IO[TriplesUploadResult] = {
    logger.info(triples.value.noSpaces)
    val cypherQuery =
      s"""
         |CALL n10s.rdf.import.inline('${triples.value.noSpaces.replace("\\", "\\\\")}', "JSON-LD")
         |""".stripMargin
    val session: Session = driver.session()
    val result =
      session.run(cypherQuery).list().asScala.map((record: Record) => s"values: ${record.values()}").mkString("\n")
    session.close()
    logger.info(result)
    IO(DeliverySuccess)
  }

}

trait Neo4jConfig {
  import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}

  val uri      = "bolt://localhost:7687"
  val user     = "neo4j"
  val password = "test"
  val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
}

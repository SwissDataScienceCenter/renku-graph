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

package ch.datascience.graph.acceptancetests.tooling

import cats.effect.concurrent.MVar
import cats.effect.{ContextShift, Fiber, IO}
import ch.datascience.graph.model.SchemaVersion
import org.apache.jena.fuseki.main.FusekiServer
import org.apache.jena.query.{DatasetFactory, QuerySolution}
import org.apache.jena.rdfconnection.RDFConnectionFactory

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

object RDFStore {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  // There's a problem with restarting Jena so this whole weirdness comes due to that fact
  private class JenaInstance {
    lazy val renkuDataSet = DatasetFactory.createTxnMem()
    lazy val connection   = RDFConnectionFactory.connect(renkuDataSet)

    private val jenaFiber = MVar.empty[IO, Fiber[IO, FusekiServer]].unsafeRunSync()

    def start(): IO[Unit] =
      for {
        _ <- contextShift.shift
        fiber <- IO {
                  FusekiServer
                    .create()
                    .loopback(true)
                    .port(3030)
                    .add("/renku", renkuDataSet)
                    .build
                    .start()
                }.start
        _ <- jenaFiber.put(fiber)
      } yield ()

    def stop(): IO[Unit] = {
      connection.close()
      renkuDataSet.close()
      jenaFiber.tryTake.flatMap {
        case None => IO.unit
        case Some(fiber) =>
          for {
            _           <- fiber.join.map(_.stop())
            cancelToken <- fiber.cancel
          } yield cancelToken
      }
    }
  }

  private val jenaReference = MVar.empty[IO, JenaInstance].unsafeRunSync()

  def start(): IO[Unit] =
    for {
      _ <- stop()
      newJena = new JenaInstance()
      _ <- jenaReference.put(newJena)
      _ <- newJena.start()
    } yield ()

  def stop(): IO[Unit] =
    for {
      maybeJena <- jenaReference.tryTake
      _         <- maybeJena.map(_.stop()).getOrElse(IO.unit)
    } yield ()

  def findAllTriplesNumber(): Int =
    jenaReference.read
      .map { jena =>
        jena.connection
          .query("SELECT (COUNT(*) as ?Triples) WHERE { ?s ?p ?o }")
          .execSelect()
          .next()
          .get("Triples")
          .asLiteral()
          .getInt
      }
      .unsafeRunSync()

  def getAllTriples: Seq[(String, String, String)] = {

    implicit class RowOps(row: QuerySolution) {
      def read(variableName: String): String =
        row.get(variableName).toString
    }

    jenaReference.read
      .map { jena =>
        jena.connection
          .query("""
                   |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                   |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                   |
                   |SELECT ?s ?p ?o WHERE {
                   |  ?s ?p ?o .
                   |}
                   |""".stripMargin)
          .execSelect()
          .asScala
          .toSeq
          .map(row => (row.read("s"), row.read("p"), row.read("o")))
      }
      .unsafeRunSync()
  }

  def doesVersionTripleExist(schemaVersion: SchemaVersion): Boolean =
    jenaReference.read
      .map { jena =>
        jena.connection
          .query(s"""
                    |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    |
                    |SELECT (COUNT(*) as ?Triples) WHERE { 
                    |  ?agentS rdfs:label "renku $schemaVersion" .
                    |}""".stripMargin)
          .execSelect()
          .next()
          .get("Triples")
          .asLiteral()
          .getInt != 0
      }
      .unsafeRunSync()
}

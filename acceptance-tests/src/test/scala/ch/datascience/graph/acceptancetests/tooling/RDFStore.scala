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

import cats.effect.IO
import cats.effect.concurrent.Ref
import ch.datascience.graph.model.SchemaVersion
import org.apache.jena.rdfconnection.RDFConnectionFactory

object RDFStore {

  import org.apache.jena.fuseki.main.FusekiServer
  import org.apache.jena.query.DatasetFactory

  // There's a problem with restarting Jena so this whole complication comes due to that fact
  private class JenaInstance {
    val renkuDataSet = DatasetFactory.createTxnMem()
    val connection   = RDFConnectionFactory.connect(renkuDataSet)
    val rdfStoreServer = FusekiServer
      .create()
      .loopback(true)
      .port(3030)
      .add("/renku", renkuDataSet)
      .build
  }

  private val jenaInstance = Ref.of[IO, JenaInstance](new JenaInstance()).unsafeRunSync()

  def start(): Unit = {
    for {
      newJena <- IO(new JenaInstance())
      _       <- jenaInstance.modify(old => newJena -> old)
      _       <- IO(newJena.rdfStoreServer.start())
    } yield ()
  }.unsafeRunSync()

  def stop(): Unit = {
    for {
      jena <- jenaInstance.get
      _ = jena.renkuDataSet.close()
      _ = jena.rdfStoreServer.stop()
    } yield ()
  }.unsafeRunSync()

  def findAllTriplesNumber(): Int =
    jenaInstance.get
      .map { jena =>
        jena.connection
          .query("SELECT (COUNT(*) as ?Triples) WHERE { ?s ?p ?o}")
          .execSelect()
          .next()
          .get("Triples")
          .asLiteral()
          .getInt
      }
      .unsafeRunSync()

  def doesVersionTripleExist(schemaVersion: SchemaVersion): Boolean =
    jenaInstance.get
      .map { jena =>
        jena.connection
          .query(s"""
                    |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    |
                    |SELECT (COUNT(*) as ?Triples) WHERE { 
                    |  ?agentS rdf:type <http://purl.org/dc/terms/SoftwareAgent> .
                    |  ?agentS rdfs:label "renku $schemaVersion".
                    |}""".stripMargin)
          .execSelect()
          .next()
          .get("Triples")
          .asLiteral()
          .getInt != 0
      }
      .unsafeRunSync()
}

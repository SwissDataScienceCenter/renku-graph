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

import org.apache.jena.rdfconnection.RDFConnectionFactory

object RDFStore {

  import org.apache.jena.fuseki.main.FusekiServer
  import org.apache.jena.query.DatasetFactory

  private lazy val renkuDataSet = DatasetFactory.createTxnMem()

  private lazy val rdfStoreServer: FusekiServer = FusekiServer
    .create()
    .loopback(true)
    .port(3030)
    .add("/renku", renkuDataSet)
    .build

  private lazy val connnection = RDFConnectionFactory.connect(renkuDataSet)

  def start(): Unit = { rdfStoreServer.start(); () }

  def stop(): Unit = { rdfStoreServer.stop(); () }

  def findAllTriplesNumber(): Int =
    connnection
      .query(
        "SELECT (COUNT(*) as ?Triples) WHERE { ?s ?p ?o}"
      )
      .execSelect()
      .next()
      .get("Triples")
      .asLiteral()
      .getInt
}

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

package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig, SparqlQueryTimeRecorder}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait TriplesVersionCreator[Interpretation[_]] {
  def updateCliVersion(): Interpretation[Unit]
}

private class IOTriplesVersionCreator(
    rdfStoreConfig:          RdfStoreConfig,
    currentCliVersion:       CliVersion,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with TriplesVersionCreator[IO] {
  import ch.datascience.rdfstore.SparqlQuery
  import eu.timepit.refined.auto._

  override def updateCliVersion(): IO[Unit] = updateWitNoResult {
    val entityId = (renkuBaseUrl / "cli-version").showAs[RdfResource]
    SparqlQuery(
      name = "cli version create",
      Set(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
        "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
      ),
      s"""|DELETE {$entityId renku:version ?o}
          |
          |INSERT { 
          |  $entityId rdf:type renku:CliVersion ;
          |  renku:version '$currentCliVersion'.
          |
          |}
          |WHERE {
          |   OPTIONAL {
          |       $entityId ?p ?o
          |   }
          |}
          |""".stripMargin
    )
  }
}

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
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait TriplesVersionFinder[Interpretation[_]] {
  def triplesUpToDate: Interpretation[Boolean]
}

private class IOTriplesVersionFinder(
    rdfStoreConfig:          RdfStoreConfig,
    executionTimeRecorder:   ExecutionTimeRecorder[IO],
    schemaVersion:           SchemaVersion,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger)
    with TriplesVersionFinder[IO] {

  import executionTimeRecorder._
  import io.circe.Decoder
  import io.circe.Decoder._

  override def triplesUpToDate: IO[Boolean] =
    measureExecutionTime {
      findCommitAgents map {
        case Nil      => false
        case versions => versions forall (_.endsWith(schemaVersion.toString))
      }
    } map logExecutionTime(withMessage = "Checking if triples are up to date done")

  private def findCommitAgents = queryExpecting[List[String]] {
    s"""|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        |PREFIX prov: <http://www.w3.org/ns/prov#>
        |
        |SELECT DISTINCT ?agent
        |WHERE {
        |  ?commit rdf:type prov:Activity ;
        |          prov:agent ?agent .
        |  ?agent rdf:type prov:SoftwareAgent .
        |}
        |""".stripMargin
  }

  private implicit lazy val agentsDecoder: Decoder[List[String]] =
    _.downField("results")
      .downField("bindings")
      .as(decodeList(ofAgents))

  private lazy val ofAgents: Decoder[String] = _.downField("agent").downField("value").as[String]
}

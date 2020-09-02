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
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.renku.jsonld.EntityId

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait ReProvisioningFlagSetter[Interpretation[_]] {
  def setUnderReProvisioningFlag(): Interpretation[Unit]
  def clearUnderReProvisioningFlag: Interpretation[Unit]
}

private case object ReProvisioningJsonLD {
  import ch.datascience.graph.Schemas._

  def id(implicit renkuBaseUrl: RenkuBaseUrl) = EntityId.of((renkuBaseUrl / "reprovisioning").toString)
  val ObjectType              = renku / "ReProvisioning"
  val CurrentlyReProvisioning = renku / "currentlyReProvisioning"
}

private class ReProvisioningFlagSetterImpl(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with ReProvisioningFlagSetter[IO] {

  import ReProvisioningJsonLD._

  override def setUnderReProvisioningFlag(): IO[Unit] = updateWitNoResult {
    SparqlQuery(
      name = "reprovisioning - flag insert",
      Set(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
        "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
      ), {
        s"""|INSERT DATA { 
            |  <${id(renkuBaseUrl)}> rdf:type <$ObjectType>;
            |                        <$CurrentlyReProvisioning> 'true'.
            |}
            |""".stripMargin
      }
    )
  }

  override def clearUnderReProvisioningFlag: IO[Unit] = ???
}

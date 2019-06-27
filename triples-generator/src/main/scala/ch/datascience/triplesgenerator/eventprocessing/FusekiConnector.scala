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

package ch.datascience.triplesgenerator.eventprocessing

import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.http.client.BasicAuthCredentials
import ch.datascience.triplesgenerator.config.{FusekiBaseUrl, FusekiUserConfig}
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFactory}

import scala.language.higherKinds
import scala.util.control.NonFatal

private abstract class FusekiConnector[Interpretation[_]] {
  def upload(rdfTriples: RDFTriples): Interpretation[Unit]
}

private class IOFusekiConnector(
    fusekiUserConfig:        FusekiUserConfig,
    fusekiConnectionBuilder: (FusekiBaseUrl, BasicAuthCredentials) => RDFConnection
)(implicit contextShift:     ContextShift[IO])
    extends FusekiConnector[IO] {

  private lazy val fusekiBaseUrl = fusekiUserConfig.fusekiBaseUrl / fusekiUserConfig.datasetName

  def upload(rdfTriples: RDFTriples): IO[Unit] =
    newConnection
      .bracket(send(rdfTriples))(closeConnection)
      .recoverWith(meaningfulError)

  private def newConnection: IO[RDFConnection] = IO {
    fusekiConnectionBuilder(fusekiBaseUrl, fusekiUserConfig.authCredentials)
  }

  private def send(rdfTriples: RDFTriples)(connection: RDFConnection): IO[Unit] =
    IO(connection.load(rdfTriples.value))

  private def closeConnection(connection: RDFConnection): IO[Unit] =
    IO(connection.close())

  private lazy val meaningfulError: PartialFunction[Throwable, IO[Unit]] = {
    case NonFatal(exception) =>
      IO.raiseError(new RuntimeException("Uploading triples to Jena failed", exception))
  }
}

private object IOFusekiConnector {

  private val fusekiConnectionBuilder: (FusekiBaseUrl, BasicAuthCredentials) => RDFConnection =
    (fusekiUrl, authCredentials) =>
      RDFConnectionFactory
        .connectPW(fusekiUrl.value, authCredentials.username.value, authCredentials.password.value)

  def apply()(implicit contextShift: ContextShift[IO]): IO[IOFusekiConnector] =
    FusekiUserConfig[IO]() map (new IOFusekiConnector(_, fusekiConnectionBuilder))
}

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

package ch.datascience.graphservice.rdfstore

import cats.MonadError
import cats.effect.{IO, Resource}
import ch.datascience.graphservice.rdfstore.RDFStoreConfig.FusekiBaseUrl
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFuseki}

import scala.language.higherKinds
import scala.util.Try
import scala.util.control.NonFatal

class RDFConnectionResourceBuilder[Interpretation[_]](
    rdfStoreConfig:          RDFStoreConfig,
    fusekiConnectionBuilder: FusekiBaseUrl => RDFConnection
)(implicit ME:               MonadError[Interpretation, Throwable]) {

  import cats.implicits._
  import rdfStoreConfig._

  def resource: Resource[IO, RDFConnection] =
    Resource
      .make(openConnection)(closeConnection)

  private def openConnection: IO[RDFConnection] =
    IO {
      fusekiConnectionBuilder(fusekiBaseUrl / datasetName)
    } recoverWith meaningfulError

  private lazy val meaningfulError: PartialFunction[Throwable, IO[RDFConnection]] = {
    case NonFatal(exception) => IO.raiseError(new RuntimeException("RDF Store cannot be accessed", exception))
  }

  private def closeConnection(connection: RDFConnection): IO[Unit] = IO {
    connection.close()
    ()
  }
}

object IORDFConnectionResourceBuilder {

  private[rdfstore] val fusekiConnectionBuilder: FusekiBaseUrl => RDFConnection =
    fusekiUrl =>
      RDFConnectionFuseki
        .create()
        .destination(fusekiUrl.toString)
        .build()

  def apply(): IO[RDFConnectionResourceBuilder[IO]] =
    for {
      config <- RDFStoreConfig[IO]()
    } yield new RDFConnectionResourceBuilder[IO](config, fusekiConnectionBuilder)
}

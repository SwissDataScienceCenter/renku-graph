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

package ch.datascience.triplesgenerator.queues.logevent

import ch.datascience.config.ServiceUrl
import ch.datascience.triplesgenerator.config.FusekiConfig
import javax.inject.{Inject, Singleton}
import org.apache.jena.rdfconnection.{RDFConnection, RDFConnectionFuseki}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@Singleton
private class FusekiConnector(
    fusekiConfig:            FusekiConfig,
    fusekiConnectionBuilder: ServiceUrl => RDFConnection
) {

  import fusekiConfig._

  @Inject() def this(fusekiConfig: FusekiConfig) = this(
    fusekiConfig,
    (fusekiUrl: ServiceUrl) =>
      RDFConnectionFuseki
        .create()
        .destination(fusekiUrl.toString)
        .build()
  )

  def upload(rdfTriples: RDFTriples)(implicit executionContext: ExecutionContext): Future[Unit] = Future {
    var connection = Option.empty[RDFConnection]
    Try {
      connection = Some(newConnection)
      connection foreach { conn =>
        conn.load(rdfTriples.value)
        conn.close()
      }
    } match {
      case Success(_) => ()
      case Failure(NonFatal(exception)) =>
        connection foreach (_.close())
        throw exception
    }
  }

  private def newConnection: RDFConnection =
    fusekiConnectionBuilder(fusekiBaseUrl / datasetName)
}

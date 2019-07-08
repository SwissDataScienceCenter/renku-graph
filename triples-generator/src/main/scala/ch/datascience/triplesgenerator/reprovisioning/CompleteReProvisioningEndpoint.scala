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

package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadError
import cats.effect._
import cats.implicits._
import ch.datascience.config.ConfigLoader
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.commands.IOEventLogMarkAllNew
import ch.datascience.http.client.BasicAuthCredentials
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.logging.ApplicationLogger
import ch.datascience.rdfstore.RdfStoreConfig
import ch.datascience.triplesgenerator.config.FusekiAdminConfig
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, Request, Response}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class CompleteReProvisioningEndpoint[Interpretation[_]](
    private[reprovisioning] val credentials: BasicAuthCredentials,
    reProvisioner:                           ReProvisioner[Interpretation]
)(implicit ME:                               MonadError[Interpretation, Throwable],
  contextShift:                              ContextShift[Interpretation],
  concurrent:                                Concurrent[Interpretation])
    extends Http4sDsl[Interpretation] {

  import reProvisioner._

  def reProvisionAll(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      _        <- validateCredentials(request)
      _        <- contextShift.shift *> concurrent.start(startReProvisioning)
      response <- Accepted(InfoMessage("RDF store re-provisioning started"))
    } yield response
  } recoverWith httpResponse

  private def validateCredentials(request: Request[Interpretation]): Interpretation[Unit] =
    request.headers.get(Authorization) match {
      case Some(Authorization(BasicCredentials(credentials.username.value, credentials.password.value))) => ME.unit
      case _                                                                                             => ME.raiseError(UnauthorizedException)
    }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case ex @ UnauthorizedException =>
      Response[Interpretation](Unauthorized)
        .withEntity[ErrorMessage](ErrorMessage(ex.getMessage))
        .pure[Interpretation]
    case NonFatal(exception) => InternalServerError(ErrorMessage(exception.getMessage))
  }
}

object IOCompleteReProvisionEndpoint extends ConfigLoader[IO] {

  def apply(transactor:          DbTransactor[IO, EventLogDB],
            fusekiAdminConfig:   IO[FusekiAdminConfig] = FusekiAdminConfig[IO](),
            rdfStoreConfig:      IO[RdfStoreConfig] = RdfStoreConfig[IO]())(
      implicit executionContext: ExecutionContext,
      contextShift:              ContextShift[IO],
      timer:                     Timer[IO]
  ): IO[CompleteReProvisioningEndpoint[IO]] =
    for {
      adminConfig      <- fusekiAdminConfig
      datasetTruncator <- rdfStoreConfig map (new IODatasetTruncator(_, ApplicationLogger))
    } yield
      new CompleteReProvisioningEndpoint(
        credentials   = adminConfig.authCredentials,
        reProvisioner = new ReProvisioner[IO](datasetTruncator, new IOEventLogMarkAllNew(transactor))
      )
}

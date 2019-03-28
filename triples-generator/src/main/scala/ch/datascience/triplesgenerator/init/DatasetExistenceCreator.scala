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

package ch.datascience.triplesgenerator.init

import cats.effect.{ContextShift, IO}
import ch.datascience.control.Throttler
import ch.datascience.http.client.{BasicAuth, IORestClient}
import ch.datascience.triplesgenerator.config.FusekiConfig

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait DatasetExistenceCreator[Interpretation[_]] {
  def createDataset(fusekiConfig: FusekiConfig): Interpretation[Unit]
}

private class IODatasetExistenceCreator(
    implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO]
) extends IORestClient(Throttler.noThrottling)
    with DatasetExistenceCreator[IO] {

  import cats.effect._
  import org.http4s.Method.POST
  import org.http4s._
  import org.http4s.dsl.io._

  override def createDataset(fusekiConfig: FusekiConfig): IO[Unit] =
    for {
      uri         <- validateUri(s"${fusekiConfig.fusekiBaseUrl}/$$/datasets")
      projectInfo <- send(postRequest(uri, fusekiConfig))(mapResponse)
    } yield projectInfo

  private def postRequest(uri: Uri, fusekiConfig: FusekiConfig): Request[IO] =
    request(POST, uri, BasicAuth(fusekiConfig.username, fusekiConfig.password))
      .withEntity(
        UrlForm(
          "dbName" -> fusekiConfig.datasetName.toString,
          "dbType" -> fusekiConfig.datasetType.toString
        )
      )

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
    case (Ok, _, _) => IO.unit
  }
}

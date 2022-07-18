/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas._
import io.renku.graph.model.{RenkuUrl, RenkuVersionPair}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

trait RenkuVersionPairUpdater[F[_]] {
  def update(versionPair: RenkuVersionPair): F[Unit]
}

private class RenkuVersionPairUpdaterImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    storeConfig:     MigrationsConnectionConfig
)(implicit renkuUrl: RenkuUrl)
    extends RdfStoreClientImpl(storeConfig)
    with RenkuVersionPairUpdater[F] {

  override def update(versionPair: RenkuVersionPair): F[Unit] =
    deleteFromDb() >> upload(versionPair.asJsonLD)

  private def deleteFromDb() = updateWithNoResult {
    SparqlQuery.of(
      name = "re-provisioning - cli and schema version remove",
      Prefixes of renku -> "renku",
      s"""|DELETE { ?s ?p ?o }
          |WHERE {
          | ?s a renku:VersionPair;
          |    ?p ?o;
          |}
          |""".stripMargin
    )
  }
}

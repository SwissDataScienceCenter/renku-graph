/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.reprovisioning

import cats.effect.Async
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas._
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{RenkuBaseUrl, RenkuVersionPair}
import io.renku.jsonld.EntityId
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

trait RenkuVersionPairUpdater[F[_]] {
  def update(versionPair: RenkuVersionPair): F[Unit]
}

private case object RenkuVersionPairJsonLD {

  def id(implicit renkuBaseUrl: RenkuBaseUrl) = EntityId.of((renkuBaseUrl / "version-pair").toString)
  val objectType                              = renku / "VersionPair"
  val cliVersion                              = renku / "cliVersion"
  val schemaVersion                           = renku / "schemaVersion"
}

private class RenkuVersionPairUpdaterImpl[F[_]: Async: Logger](rdfStoreConfig: RdfStoreConfig,
                                                               renkuBaseUrl: RenkuBaseUrl,
                                                               timeRecorder: SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with RenkuVersionPairUpdater[F] {

  override def update(versionPair: RenkuVersionPair): F[Unit] = updateWithNoResult {
    val entityId = (renkuBaseUrl / "version-pair").showAs[RdfResource]
    SparqlQuery.of(
      name = "ReProvisioning - cli and schema version create",
      Prefixes.of(
        rdf   -> "rdf",
        renku -> "renku"
      ),
      s"""|DELETE {$entityId <${RenkuVersionPairJsonLD.cliVersion}> ?o .
          |        $entityId <${RenkuVersionPairJsonLD.schemaVersion}> ?q .
          |}
          |
          |INSERT{ 
          |  <${RenkuVersionPairJsonLD.id(renkuBaseUrl)}> rdf:type <${RenkuVersionPairJsonLD.objectType}> ;
          |                                         <${RenkuVersionPairJsonLD.cliVersion}> '${versionPair.cliVersion}' ;
          |                                          <${RenkuVersionPairJsonLD.schemaVersion}> '${versionPair.schemaVersion}'
          |}
          |WHERE {
          |  OPTIONAL {
          |    $entityId ?p ?o;
          |              ?r ?q.
          |  }
          |}
          |""".stripMargin
    )
  }
}

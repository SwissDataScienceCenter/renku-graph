/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.jsonld.EntityId
import io.renku.triplesstore._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private trait TriplesRemover[F[_]] {
  def removeAllTriples(): F[Unit]
}

private class TriplesRemoverImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    storeConfig:    ProjectsConnectionConfig,
    idleTimeout:    Duration = 11 minutes,
    requestTimeout: Duration = 10 minutes
) extends TSClientImpl(storeConfig,
                       idleTimeoutOverride = idleTimeout.some,
                       requestTimeoutOverride = requestTimeout.some
    )
    with TriplesRemover[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.GraphClass
  import io.renku.graph.model.Schemas._
  import io.renku.triplesstore.client.syntax._

  override def removeAllTriples(): F[Unit] =
    queryExpecting[Option[EntityId]](findGraph) >>= {
      case Some(graphId) => updateWithNoResult(remove(graphId)) >> removeAllTriples()
      case None          => ().pure[F]
    }

  private val findGraph = SparqlQuery.of(
    name = "triples remove - count",
    Prefixes of renku -> "renku",
    s"""|SELECT DISTINCT ?graph
        |WHERE {
        |  GRAPH ?graph { ?s ?p ?o }
        |  FILTER (?graph != ${GraphClass.ProjectViewedTimes.id.sparql})
        |}
        |LIMIT 1
        |""".stripMargin
  )

  private def remove(graphId: EntityId) = SparqlQuery.of(
    name = "triples remove - delete",
    Prefixes of renku -> "renku",
    s"DROP GRAPH ${graphId.sparql}"
  )

  private implicit val graphIdDecoder: Decoder[Option[EntityId]] = ResultsDecoder[List, EntityId] { implicit cur =>
    extract[String]("graph").map(EntityId.of(_))
  }.map(_.headOption)
}

private object TriplesRemoverImpl {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      config: Config = ConfigFactory.load()
  ): F[TriplesRemover[F]] = ProjectsConnectionConfig[F](config).map(new TriplesRemoverImpl(_))
}

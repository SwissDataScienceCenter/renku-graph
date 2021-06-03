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

package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.Schemas.rdf
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.RenkuVersionPair
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

trait RenkuVersionPairFinder[Interpretation[_]] {

  def find(): Interpretation[Option[RenkuVersionPair]]

}

private class RenkuVersionPairFinderImpl[Interpretation[_]: ConcurrentEffect: Timer](
    rdfStoreConfig: RdfStoreConfig,
    renkuBaseUrl:   RenkuBaseUrl,
    logger:         Logger[Interpretation],
    timeRecorder:   SparqlQueryTimeRecorder[Interpretation]
)(implicit
    executionContext: ExecutionContext
) extends RdfStoreClientImpl[Interpretation](rdfStoreConfig, logger, timeRecorder)
    with RenkuVersionPairFinder[Interpretation] {

  override def find(): Interpretation[Option[RenkuVersionPair]] = queryExpecting[List[RenkuVersionPair]] {
    val entityId = (renkuBaseUrl / "version-pair").showAs[RdfResource]
    SparqlQuery.of(
      name = "version pair find",
      Prefixes.of(rdf -> "rdf"),
      s"""|SELECT DISTINCT ?schemaVersion ?cliVersion
          |WHERE {
          |  $entityId rdf:type <${RenkuVersionPairJsonLD.objectType}>;
          |      <${RenkuVersionPairJsonLD.schemaVersion}> ?schemaVersion;
          |      <${RenkuVersionPairJsonLD.cliVersion}> ?cliVersion.
          |}
          |""".stripMargin
    )
  }.flatMap {
    case Nil         => Option.empty[RenkuVersionPair].pure[Interpretation]
    case head :: Nil => head.some.pure[Interpretation]
    case versionPairs =>
      new IllegalStateException(s"Too many Version pair found: $versionPairs")
        .raiseError[Interpretation, Option[RenkuVersionPair]]
  }
}

private object RenkuVersionPairFinder {
  def apply(rdfStoreConfig: RdfStoreConfig,
            renkuBaseUrl:   RenkuBaseUrl,
            logger:         Logger[IO],
            timeRecorder:   SparqlQueryTimeRecorder[IO]
  )(implicit
      executionContext: ExecutionContext,
      concurrentEffect: ConcurrentEffect[IO],
      timer:            Timer[IO]
  ): IO[RenkuVersionPairFinderImpl[IO]] = IO(
    new RenkuVersionPairFinderImpl[IO](rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  )
}

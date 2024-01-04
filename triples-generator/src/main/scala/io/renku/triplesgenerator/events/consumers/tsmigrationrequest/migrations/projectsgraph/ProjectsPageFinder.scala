/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations
package projectsgraph

import cats.Monad
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.{RenkuUrl, Schemas, projects}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger
import tooling._

private trait ProjectsPageFinder[F[_]] {
  def nextProjectsPage(): F[List[projects.Slug]]
}

private object ProjectsPageFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProjectsPageFinder[F]] = for {
    implicit0(ru: RenkuUrl) <- RenkuUrlLoader[F]()
    recordsFinder           <- MigrationsConnectionConfig.fromConfig[F]().map(RecordsFinder[F](_))
  } yield new ProjectsPageFinderImpl(recordsFinder)
}

private class ProjectsPageFinderImpl[F[_]: Monad](recordsFinder: RecordsFinder[F])(implicit
    ru: RenkuUrl
) extends ProjectsPageFinder[F]
    with Schemas {

  import ResultsDecoder._
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import recordsFinder._

  private val pageSize: Int = 50

  override def nextProjectsPage(): F[List[projects.Slug]] =
    findRecords[projects.Slug](query)

  private lazy val query = SparqlQuery.ofUnsafe(
    show"${ProvisionProjectsGraph.name} - find projects",
    Prefixes of renku -> "renku",
    sparql"""|SELECT DISTINCT ?slug
             |WHERE {
             |  ${ProvisionProjectsGraph.name.asEntityId} renku:toBeMigrated ?slug
             |}
             |ORDER BY ?slug
             |LIMIT $pageSize
             |""".stripMargin
  )

  private implicit lazy val decoder: Decoder[List[projects.Slug]] = ResultsDecoder[List, projects.Slug] {
    implicit cur => extract[projects.Slug]("slug")
  }
}

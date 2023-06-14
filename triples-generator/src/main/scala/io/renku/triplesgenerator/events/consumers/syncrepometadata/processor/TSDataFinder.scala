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

package io.renku.triplesgenerator.events.consumers.syncrepometadata
package processor

import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.projects
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{ResultsDecoder, SparqlQuery, TSClient}

private trait TSDataFinder[F[_]] {
  def fetchTSData(path: projects.Path): F[Option[DataExtract]]
}

private class TSDataFinderImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends TSDataFinder[F] {

  import ResultsDecoder._
  import io.circe.Decoder

  override def fetchTSData(path: projects.Path): F[Option[DataExtract]] =
    tsClient.queryExpecting[Option[DataExtract]] {
      SparqlQuery.ofUnsafe(
        show"$categoryName: find data",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?path ?name
                 |WHERE {
                 |  BIND (${path.asObject} AS ?path)
                 |  GRAPH ?id {
                 |    ?id a schema:Project;
                 |        renku:projectPath ?path;
                 |        schema:name ?name
                 |  }
                 |}
                 |LIMIT 1
                 |""".stripMargin
      )
    }(decoder(path))

  private def decoder(path: projects.Path): Decoder[Option[DataExtract]] =
    ResultsDecoder[Option, DataExtract] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      for {
        path <- extract[projects.Path]("path")
        name <- extract[projects.Name]("name")
      } yield DataExtract(path, name)
    }(toOption(show"Multiple projects or values for '$path'"))
}

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

package io.renku.knowledgegraph.entities

import cats.effect.Async
import cats.syntax.all._
import io.renku.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQueryTimeRecorder}
import model._
import org.typelevel.log4cats.Logger

private trait EntitiesFinder[F[_]] {
  def findEntities(): F[List[Entity]]
}

private class EntitiesFinderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl[F](rdfStoreConfig, timeRecorder)
    with EntitiesFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas.{renku, schema}
  import io.renku.graph.model.projects
  import io.renku.rdfstore.SparqlQuery
  import io.renku.rdfstore.SparqlQuery.Prefixes

  override def findEntities(): F[List[Entity]] =
    queryExpecting[List[Entity]](using = query())

  private def query() = SparqlQuery.of(
    name = "cross-entity search",
    Prefixes.of(schema -> "schema", renku -> "renku"),
    s"""|SELECT ?entityType ?name ?path ?visibility ?dateCreated ?maybeCreatorName (GROUP_CONCAT(?keyword; separator=',') AS ?keywords) ?maybeDescription
        |WHERE {
        |  BIND ('project' AS ?entityType)
        |  ?projectId a schema:Project;
        |             schema:name ?name;
        |             renku:projectPath ?path;
        |             renku:projectVisibility ?visibility;
        |             schema:dateCreated ?dateCreated.
        |  OPTIONAL { ?projectId schema:creator/schema:name ?maybeCreatorName }
        |  OPTIONAL { ?projectId schema:keywords ?keyword }
        |  OPTIONAL { ?projectId schema:description ?maybeDescription }
        |}
        |GROUP BY ?entityType ?name ?path ?visibility ?dateCreated ?maybeCreatorName ?maybeDescription
        |""".stripMargin
  )

  private implicit lazy val recordsDecoder: Decoder[List[Entity]] = {
    import Decoder._
    import io.circe.DecodingFailure
    import io.renku.graph.model.projects._
    import io.renku.graph.model.users
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val entity: Decoder[Entity] = { cursor =>
      val toListOfKeywords: Option[String] => Decoder.Result[List[Keyword]] =
        _.map(_.split(',').toList.map(Keyword.from).sequence.map(_.sorted)).sequence
          .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
          .map(_.getOrElse(List.empty))

      cursor.downField("entityType").downField("value").as[String] >>= { case "project" =>
        for {
          name             <- cursor.downField("name").downField("value").as[projects.Name]
          path             <- cursor.downField("path").downField("value").as[projects.Path]
          visibility       <- cursor.downField("visibility").downField("value").as[projects.Visibility]
          dateCreated      <- cursor.downField("dateCreated").downField("value").as[DateCreated]
          maybeCreatorName <- cursor.downField("maybeCreatorName").downField("value").as[Option[users.Name]]
          keywords <- cursor.downField("keywords").downField("value").as[Option[String]].flatMap(toListOfKeywords)
          maybeDescription <- cursor.downField("maybeDescription").downField("value").as[Option[Description]]
        } yield Project(name, path, visibility, dateCreated, maybeCreatorName, keywords, maybeDescription)
      }
    }

    _.downField("results").downField("bindings").as(decodeList(entity))
  }
}

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
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import eu.timepit.refined.auto._
import io.circe.DecodingFailure
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

private trait TSDataFinder[F[_]] {
  def fetchTSData(path: projects.Path): F[Option[DataExtract.TS]]
}

private object TSDataFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](config: Config): F[TSDataFinder[F]] =
    ProjectsConnectionConfig[F](config).map(TSClient[F](_)).map(new TSDataFinderImpl(_))
}

private class TSDataFinderImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends TSDataFinder[F] {

  import ResultsDecoder._
  import io.circe.Decoder

  override def fetchTSData(path: projects.Path): F[Option[DataExtract.TS]] =
    tsClient.queryExpecting[Option[DataExtract.TS]] {
      SparqlQuery.ofUnsafe(
        show"$categoryName: find data",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?id ?path ?name ?visibility ?maybeDesc
                 |  (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
                 |  (GROUP_CONCAT(DISTINCT ?encodedImageUrl; separator=',') AS ?images)
                 |WHERE {
                 |  BIND (${path.asObject} AS ?path)
                 |  GRAPH ?id {
                 |    ?id a schema:Project;
                 |        renku:projectPath ?path;
                 |        schema:name ?name;
                 |        renku:projectVisibility ?visibility.
                 |    OPTIONAL { ?id schema:description ?maybeDesc }
                 |    OPTIONAL { ?dsId schema:keywords ?keyword }
                 |    OPTIONAL {
                 |      ?id schema:image ?imageId.
                 |      ?imageId schema:position ?imagePosition;
                 |               schema:contentUrl ?imageUrl.
                 |      BIND(CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
                 |    }
                 |  }
                 |}
                 |GROUP BY ?id ?path ?name ?visibility ?maybeDesc
                 |LIMIT 1
                 |""".stripMargin
      )
    }(decoder(path))

  private def decoder(path: projects.Path): Decoder[Option[DataExtract.TS]] =
    ResultsDecoder[Option, DataExtract.TS] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._

      val toSetOfKeywords: Option[String] => Decoder.Result[Set[projects.Keyword]] =
        _.map(_.split(',').toList.distinct.map(projects.Keyword.from).sequence).sequence
          .leftMap(ex =>
            DecodingFailure(DecodingFailure.Reason.CustomReason(s"Cannot decode keywords: ${ex.getMessage}"), cur)
          )
          .map(_.getOrElse(List.empty).toSet)

      def toListOfImageUris: Option[String] => Decoder.Result[List[ImageUri]] =
        _.map(ImageUri.fromSplitString(','))
          .map(_.leftMap(ex => DecodingFailure(ex.getMessage, Nil)))
          .getOrElse(Nil.asRight)

      for {
        id         <- extract[projects.ResourceId]("id")
        path       <- extract[projects.Path]("path")
        name       <- extract[projects.Name]("name")
        visibility <- extract[projects.Visibility]("visibility")
        maybeDesc  <- extract[Option[projects.Description]]("maybeDesc")
        keywords   <- extract[Option[String]]("keywords") >>= toSetOfKeywords
        images     <- extract[Option[String]]("images") >>= toListOfImageUris
      } yield DataExtract.TS(id, path, name, visibility, maybeDesc, keywords, images)
    }(toOption(show"Multiple projects or values for '$path'"))
}

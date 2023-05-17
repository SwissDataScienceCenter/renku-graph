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

package io.renku.entities.searchgraphs.projects
package commands

import cats.effect.Async
import io.renku.entities.searchgraphs.PersonInfo
import io.renku.graph.model.{persons, projects}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait SearchInfoFetcher[F[_]] {
  def fetchTSSearchInfo(projectId: projects.ResourceId): F[Option[ProjectSearchInfo]]
}

private object SearchInfoFetcher {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): SearchInfoFetcher[F] = new SearchInfoFetcherImpl[F](TSClient[F](connectionConfig))
}

private class SearchInfoFetcherImpl[F[_]](tsClient: TSClient[F]) extends SearchInfoFetcher[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.GraphClass
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore._
  import io.renku.triplesstore.client.syntax._
  import tsClient.queryExpecting

  override def fetchTSSearchInfo(projectId: projects.ResourceId): F[Option[ProjectSearchInfo]] =
    queryExpecting[Option[ProjectSearchInfo]](query(projectId))

  private def query(resourceId: projects.ResourceId) = SparqlQuery.of(
    name = "project search info",
    Prefixes of (renku -> "renku", schema -> "schema"),
    sparql"""|SELECT DISTINCT ?id ?name ?path ?visibility ?dateCreated ?maybeDescription
             |                ?maybeCreatorId ?maybeCreatorName
             |                (GROUP_CONCAT(?keyword; separator=$rowsSeparator) AS ?keywords)
             |                (GROUP_CONCAT(?image; separator=$rowsSeparator) AS ?images)
             |WHERE {
             |  BIND (${resourceId.asEntityId} AS ?id)
             |  GRAPH ${GraphClass.Projects.id} {
             |    ?id schema:name ?name;
             |        renku:projectPath ?path;
             |        renku:projectVisibility ?visibility;
             |        schema:dateCreated ?dateCreated.
             |    OPTIONAL { ?id schema:description ?maybeDescription }
             |    OPTIONAL {
             |      ?id schema:creator ?maybeCreatorId.
             |      ?maybeCreatorId schema:name ?maybeCreatorName.
             |    }
             |    OPTIONAL { ?id schema:keywords ?keyword }
             |    OPTIONAL {
             |      ?id schema:image ?imageId.
             |      ?imageId schema:position ?imagePosition;
             |               schema:contentUrl ?imageUrl.
             |      BIND (CONCAT(STR(?imageId), STR(';;'), STR(?imagePosition), STR(';;'), STR(?imageUrl)) AS ?image)
             |    }
             |  }
             |}
             |GROUP BY ?id ?name ?path ?visibility ?dateCreated ?maybeDescription
             |         ?maybeCreatorId ?maybeCreatorName
             |""".stripMargin
  )

  private lazy val rowsSeparator = '\u0000'

  private implicit lazy val recordsDecoder: Decoder[Option[ProjectSearchInfo]] =
    ResultsDecoder[Option, ProjectSearchInfo] { implicit cur =>
      import Decoder._
      import io.circe.DecodingFailure
      import io.renku.graph.model.{images, projects}
      import io.renku.tinytypes.json.TinyTypeDecoders._

      val splitRows: String => List[String] = _.split(rowsSeparator).toList.distinct

      def decode[O](map: String => Decoder.Result[O], sort: List[O] => List[O]): String => Decoder.Result[List[O]] =
        splitRows(_).map(map).sequence.map(sort)

      val toKeyword: String => Decoder.Result[projects.Keyword] = str =>
        projects.Keyword
          .from(str)
          .leftMap(ex => DecodingFailure(DecodingFailure.Reason.CustomReason(ex.getMessage), cur))

      val toListOfKeywords: Option[String] => Decoder.Result[List[projects.Keyword]] =
        _.map(decode[projects.Keyword](map = toKeyword, sort = _.sorted))
          .getOrElse(List.empty.asRight[DecodingFailure])

      val toImage: String => Decoder.Result[images.Image] = {
        case s"$id;;$position;;$url" =>
          (images.ImageResourceId
             .from(id)
             .leftMap(ex => DecodingFailure(DecodingFailure.Reason.CustomReason(ex.getMessage), cur)),
           images.ImageUri
             .from(url)
             .leftMap(ex => DecodingFailure(DecodingFailure.Reason.CustomReason(ex.getMessage), cur)),
           Either
             .fromOption(position.toIntOption,
                         DecodingFailure(DecodingFailure.Reason.CustomReason("Image position not an Int"), cur)
             )
             .flatMap(
               images.ImagePosition
                 .from(_)
                 .leftMap(ex => DecodingFailure(DecodingFailure.Reason.CustomReason(ex.getMessage), cur))
             )
          ).mapN(images.Image(_, _, _))
        case other =>
          DecodingFailure(DecodingFailure.Reason.CustomReason(s"'$other' not a valid image record"), cur)
            .asLeft[images.Image]
      }

      val toListOfImages: Option[String] => Decoder.Result[List[images.Image]] =
        _.map(decode[images.Image](map = toImage, sort = _.sortBy(_.position)))
          .getOrElse(List.empty.asRight[DecodingFailure])

      for {
        id               <- extract[projects.ResourceId]("id")
        name             <- extract[projects.Name]("name")
        path             <- extract[projects.Path]("path")
        visibility       <- extract[projects.Visibility]("visibility")
        dateCreated      <- extract[projects.DateCreated]("dateCreated")
        maybeDescription <- extract[Option[projects.Description]]("maybeDescription")
        maybeCreatorId   <- extract[Option[persons.ResourceId]]("maybeCreatorId")
        maybeCreatorName <- extract[Option[persons.Name]]("maybeCreatorName")
        keywords         <- extract[Option[String]]("keywords") >>= toListOfKeywords
        images           <- extract[Option[String]]("images") >>= toListOfImages
      } yield ProjectSearchInfo(id,
                                name,
                                path,
                                visibility,
                                dateCreated,
                                (maybeCreatorId -> maybeCreatorName).mapN(PersonInfo(_, _)),
                                keywords,
                                maybeDescription,
                                images
      )
    }
}

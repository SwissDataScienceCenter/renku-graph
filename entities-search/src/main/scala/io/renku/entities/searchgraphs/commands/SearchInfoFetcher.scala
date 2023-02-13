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

package io.renku.entities.searchgraphs
package commands

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.datasets.TopmostSameAs
import io.renku.graph.model.{GraphClass, projects}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClientImpl}
import org.typelevel.log4cats.Logger

private trait SearchInfoFetcher[F[_]] {
  def fetchTSSearchInfos(projectId: projects.ResourceId): F[List[SearchInfo]]
}

private object SearchInfoFetcher {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[SearchInfoFetcher[F]] =
    ProjectsConnectionConfig[F]().map(new SearchInfoFetcherImpl[F](_))
}

private class SearchInfoFetcherImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig)
    extends TSClientImpl(storeConfig)
    with SearchInfoFetcher[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore._
  import io.renku.triplesstore.client.syntax._

  override def fetchTSSearchInfos(projectId: projects.ResourceId): F[List[SearchInfo]] =
    queryExpecting[List[SearchInfo]](query(projectId))

  private def query(resourceId: projects.ResourceId) = SparqlQuery.of(
    name = "ds search infos",
    Prefixes of (renku -> "renku", schema -> "schema"),
    s"""|SELECT DISTINCT ?topSameAs ?name ?visibility ?maybeDescription
        |                ?maybeDateCreated ?maybeDatePublished ?maybeDateModified
        |                (GROUP_CONCAT(?creator; separator='$rowsSeparator') AS ?creators)
        |                (GROUP_CONCAT(?keyword; separator='$rowsSeparator') AS ?keywords)
        |                (GROUP_CONCAT(?image; separator='$rowsSeparator') AS ?images)
        |                (GROUP_CONCAT(?link; separator='$rowsSeparator') AS ?links)
        |WHERE {
        |  {
        |    SELECT DISTINCT ?topSameAs
        |    WHERE {
        |      GRAPH ${GraphClass.Datasets.id.asSparql.sparql} {
        |        ?linkId a renku:DatasetProjectLink;
        |                renku:project ${resourceId.asEntityId.asSparql.sparql}.
        |        ?topSameAs renku:datasetProjectLink ?linkId
        |      }
        |    }
        |  } {
        |    GRAPH ${GraphClass.Datasets.id.asSparql.sparql} {
        |      ?topSameAs renku:slug ?name;
        |                 renku:projectVisibility ?visibility.
        |      OPTIONAL { ?topSameAs schema:description ?maybeDescription }
        |      OPTIONAL { ?topSameAs schema:dateCreated ?maybeDateCreated }
        |      OPTIONAL { ?topSameAs schema:datePublished ?maybeDatePublished }
        |      OPTIONAL { ?topSameAs schema:dateModified ?maybeDateModified }
        |      {
        |        ?topSameAs schema:creator ?creatorId.
        |        ?creatorId schema:name ?creatorName.
        |        BIND (CONCAT(STR(?creatorId), STR(';;'), STR(?creatorName)) AS ?creator)
        |      }
        |      OPTIONAL { ?topSameAs schema:keywords ?keyword }
        |      OPTIONAL {
        |        ?topSameAs schema:image ?imageId.
        |        ?imageId schema:position ?imagePosition;
        |                 schema:contentUrl ?imageUrl.
        |        BIND (CONCAT(STR(?imageId), STR(';;'), STR(?imagePosition), STR(';;'), STR(?imageUrl)) AS ?image)
        |      }
        |      {
        |        ?topSameAs renku:datasetProjectLink ?linkId.
        |        ?linkId renku:project ?linkProjectId;
        |                renku:dataset ?linkDatasetId.
        |        BIND (CONCAT(STR(?linkId), STR(';;'), STR(?linkProjectId), STR(';;'), STR(?linkDatasetId)) AS ?link)
        |      }
        |    }
        |  }
        |}
        |GROUP BY ?topSameAs ?name ?visibility ?maybeDescription
        |         ?maybeDateCreated ?maybeDatePublished ?maybeDateModified
        |ORDER BY ?name
        |""".stripMargin
  )

  private lazy val rowsSeparator = "##"

  private implicit lazy val recordsDecoder: Decoder[List[SearchInfo]] = ResultsDecoder[List, SearchInfo] {
    implicit cur =>
      import Decoder._
      import io.circe.DecodingFailure
      import io.renku.graph.model.{datasets, images, persons, projects}
      import io.renku.tinytypes.json.TinyTypeDecoders._

      val splitRows: String => List[String] = _.split(rowsSeparator).toList.distinct

      def decode[O](map: String => Decoder.Result[O], sort: List[O] => List[O]): String => Decoder.Result[List[O]] =
        splitRows(_).map(map).sequence.map(sort)

      def toNonEmptyList[O](noElemMessage: String): List[O] => Decoder.Result[NonEmptyList[O]] = {
        case Nil => DecodingFailure(DecodingFailure.Reason.CustomReason(noElemMessage), cur).asLeft[NonEmptyList[O]]
        case head :: tail => NonEmptyList.of(head, tail: _*).asRight
      }

      val toKeyword: String => Decoder.Result[datasets.Keyword] = str =>
        datasets.Keyword
          .from(str)
          .leftMap(ex => DecodingFailure(DecodingFailure.Reason.CustomReason(ex.getMessage), cur))

      val toListOfKeywords: Option[String] => Decoder.Result[List[datasets.Keyword]] =
        _.map(decode[datasets.Keyword](map = toKeyword, sort = _.sorted))
          .getOrElse(List.empty.asRight[DecodingFailure])

      val toCreator: String => Decoder.Result[PersonInfo] = {
        case s"$id;;$name" =>
          (persons.ResourceId.from(id) -> persons.Name.from(name))
            .mapN(PersonInfo(_, _))
            .leftMap(ex => DecodingFailure(DecodingFailure.Reason.CustomReason(ex.getMessage), cur))
        case other =>
          DecodingFailure(DecodingFailure.Reason.CustomReason(s"'$other' not a valid creator record"), cur)
            .asLeft[PersonInfo]
      }

      def toListOfCreators(implicit topmostSameAs: TopmostSameAs): String => Decoder.Result[NonEmptyList[PersonInfo]] =
        decode[PersonInfo](map = toCreator, sort = _.sortBy(_.name))(_)
          .flatMap(toNonEmptyList(s"No creators found for $topmostSameAs"))

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

      val toLink: String => Decoder.Result[Link] = {
        case s"$id;;$projectId;;$datasetId" =>
          (links.ResourceId
             .from(id)
             .leftMap(ex => DecodingFailure(DecodingFailure.Reason.CustomReason(ex.getMessage), cur)),
           datasets.ResourceId
             .from(datasetId)
             .leftMap(ex => DecodingFailure(DecodingFailure.Reason.CustomReason(ex.getMessage), cur)),
           projects.ResourceId
             .from(projectId)
             .leftMap(ex => DecodingFailure(DecodingFailure.Reason.CustomReason(ex.getMessage), cur))
          ).mapN(Link(_, _, _))
        case other =>
          DecodingFailure(DecodingFailure.Reason.CustomReason(s"'$other' not a valid link record"), cur).asLeft[Link]
      }

      def toListOfLinks(implicit topmostSameAs: TopmostSameAs): String => Decoder.Result[NonEmptyList[Link]] =
        decode[Link](map = toLink, sort = _.sortBy(_.projectId))(_)
          .flatMap(toNonEmptyList(s"No links found for $topmostSameAs"))

      def toCreatedOrPublished(maybeDateCreated:   Option[datasets.DateCreated],
                               maybeDatePublished: Option[datasets.DatePublished]
      )(implicit topSameAs: TopmostSameAs): Either[DecodingFailure, datasets.CreatedOrPublished] = Either.fromOption(
        maybeDateCreated orElse maybeDatePublished,
        ifNone = DecodingFailure(
          DecodingFailure.Reason.CustomReason(s"neither dateCreated nor datePublished for $topSameAs"),
          cur
        )
      )

      for {
        implicit0(topSameAs: datasets.TopmostSameAs) <- extract[datasets.TopmostSameAs]("topSameAs")
        name                                         <- extract[datasets.Name]("name")
        visibility                                   <- extract[projects.Visibility]("visibility")
        maybeDescription                             <- extract[Option[datasets.Description]]("maybeDescription")
        maybeDateCreated                             <- extract[Option[datasets.DateCreated]]("maybeDateCreated")
        maybeDatePublished                           <- extract[Option[datasets.DatePublished]]("maybeDatePublished")
        createdOrPublished                           <- toCreatedOrPublished(maybeDateCreated, maybeDatePublished)
        maybeDateModified                            <- extract[Option[datasets.DateModified]]("maybeDateModified")
        creators                                     <- extract[String]("creators") >>= toListOfCreators
        keywords                                     <- extract[Option[String]]("keywords") >>= toListOfKeywords
        images                                       <- extract[Option[String]]("images") >>= toListOfImages
        links                                        <- extract[String]("links") >>= toListOfLinks
      } yield SearchInfo(topSameAs,
                         name,
                         visibility,
                         createdOrPublished,
                         maybeDateModified,
                         creators,
                         keywords,
                         maybeDescription,
                         images,
                         links
      )
  }
}

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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.projects

import cats.data.{NonEmptyList => Nel}
import cats.effect.Async
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.{persons, projects}
import io.renku.graph.model.images.ImageResourceId
import io.renku.graph.model.versions.CliVersion
import io.renku.graph.model.views.RdfResource
import io.renku.triplesstore._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.typelevel.log4cats.Logger

private trait KGProjectFinder[F[_]] {
  def find(resourceId: projects.ResourceId): F[Option[ProjectMutableData]]
}

private class KGProjectFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    connectionConfig: ProjectsConnectionConfig
) extends TSClientImpl(connectionConfig)
    with KGProjectFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import Decoder._
  import ResultsDecoder._
  import io.renku.graph.model.Schemas._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def find(resourceId: projects.ResourceId): F[Option[ProjectMutableData]] =
    queryExpecting(selectQuery = query(resourceId))(decoder(resourceId))

  private def query(resourceId: projects.ResourceId) = SparqlQuery.of(
    name = "transformation - find project",
    Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
    s"""|SELECT DISTINCT ?name (GROUP_CONCAT(?dateCreated; separator=',') AS ?createdDates) ?maybeParent ?visibility ?maybeDescription
        |  (GROUP_CONCAT(?keyword; separator=',') AS ?keywords) ?maybeAgent ?maybeCreatorId
        |  (GROUP_CONCAT(?imageId; separator=',') AS ?images)
        |WHERE {
        |  GRAPH ${resourceId.showAs[RdfResource]} {
        |    BIND (${resourceId.showAs[RdfResource]} AS ?id)
        |    ?id a schema:Project;
        |        schema:name ?name;
        |        schema:dateCreated ?dateCreated;
        |        renku:projectVisibility ?visibility.
        |    OPTIONAL { ?id schema:description ?maybeDescription }
        |    OPTIONAL { ?id schema:keywords ?keyword }
        |    OPTIONAL { ?id prov:wasDerivedFrom ?maybeParent }
        |    OPTIONAL { ?id schema:agent ?maybeAgent }
        |    OPTIONAL { ?id schema:creator ?maybeCreatorId }
        |    OPTIONAL { ?id schema:image ?imageId }
        |  }
        |}
        |GROUP BY ?name ?dateCreated ?maybeParent ?visibility ?maybeDescription ?maybeAgent ?maybeCreatorId
        |LIMIT 1
        |""".stripMargin
  )

  private def decoder(resourceId: projects.ResourceId): Decoder[Option[ProjectMutableData]] =
    ResultsDecoder[Option, ProjectMutableData] { implicit cur =>
      val toSetOfKeywords: Option[String] => Decoder.Result[Set[projects.Keyword]] =
        _.map(_.split(',').toList.map(projects.Keyword.from).sequence.map(_.toSet)).sequence
          .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
          .map(_.getOrElse(Set.empty))

      val toListOfDates: Option[String] => Decoder.Result[Nel[projects.DateCreated]] =
        _.toList
          .flatMap(_.split(',').toList.distinct)
          .map(io.circe.Json.fromString)
          .traverse(_.as[projects.DateCreated])
          .flatMap(list =>
            Nel.fromList(list).toRight(DecodingFailure(show"No dateCreated provided for project $resourceId", Nil))
          )

      val toListOfImageResourceId: Option[String] => Decoder.Result[List[ImageResourceId]] =
        _.toList
          .flatMap(_.split(',').filter(_.nonEmpty).toSet.toList.sorted)
          .traverse(ImageResourceId.from)
          .leftMap(ex => DecodingFailure(ex.getMessage, Nil))

      for {
        name             <- extract[projects.Name]("name")
        dateCreated      <- extract[Option[String]]("createdDates") >>= toListOfDates
        maybeParent      <- extract[Option[projects.ResourceId]]("maybeParent")
        visibility       <- extract[projects.Visibility]("visibility")
        maybeDescription <- extract[Option[projects.Description]]("maybeDescription")
        keywords         <- extract[Option[String]]("keywords") >>= toSetOfKeywords
        maybeAgent       <- extract[Option[CliVersion]]("maybeAgent")
        maybeCreatorId   <- extract[Option[persons.ResourceId]]("maybeCreatorId")
        images           <- extract[Option[String]]("images") >>= toListOfImageResourceId
      } yield ProjectMutableData(
        name,
        dateCreated,
        maybeParent,
        visibility,
        maybeDescription,
        keywords,
        maybeAgent,
        maybeCreatorId,
        images
      )
    }(toOption(s"Multiple projects or values for '$resourceId'"))
}

private object KGProjectFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGProjectFinder[F]] =
    ProjectsConnectionConfig[F]().map(new KGProjectFinderImpl(_))
}

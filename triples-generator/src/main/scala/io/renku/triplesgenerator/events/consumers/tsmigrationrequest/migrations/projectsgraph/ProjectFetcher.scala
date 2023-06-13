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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.projectsgraph

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.{Decoder, DecodingFailure}
import io.renku.graph.model.Schemas._
import io.renku.graph.model.entities.{NonRenkuProject, Person, Project}
import io.renku.graph.model.images.{Image, ImageUri}
import io.renku.graph.model.{GraphClass, persons, projects}
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

private trait ProjectFetcher[F[_]] {
  def fetchProject(path: projects.Path): F[Option[Project]]
}

private object ProjectFetcher {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProjectFetcher[F]] =
    ProjectsConnectionConfig[F]().map(TSClient[F](_)).map(new ProjectFetcherImpl[F](_))
}

private class ProjectFetcherImpl[F[_]: Async](tsClient: TSClient[F]) extends ProjectFetcher[F] {

  override def fetchProject(path: projects.Path): F[Option[Project]] = tsClient.queryExpecting[Option[Project]](
    SparqlQuery.ofUnsafe(
      show"${ProvisionProjectsGraph.name} - find projects",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|SELECT DISTINCT ?id ?path ?name ?maybeDesc ?dateCreated ?maybeCreatorId ?maybeCreatorName ?visibility
               |  (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
               |  (GROUP_CONCAT(DISTINCT ?encodedImageUrl; separator=',') AS ?images)
               |WHERE {
               |  BIND (${path.asObject} AS ?path)
               |  GRAPH ?id {
               |    ?id a schema:Project;
               |        renku:projectPath ?path;
               |        schema:name ?name;
               |        schema:dateCreated ?dateCreated;
               |        renku:projectVisibility ?visibility.
               |
               |    OPTIONAL { ?id schema:description ?maybeDesc }
               |
               |    OPTIONAL {
               |      ?id schema:creator ?maybeCreatorId.
               |      GRAPH ${GraphClass.Persons.id} {
               |        ?maybeCreatorId schema:name ?maybeCreatorName
               |      }
               |    }
               |
               |    OPTIONAL { ?id schema:keywords ?keyword }
               |
               |    OPTIONAL {
               |      ?id schema:image ?imageId.
               |      ?imageId schema:position ?imagePosition;
               |               schema:contentUrl ?imageUrl.
               |      BIND (CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
               |    }
               |  }
               |}
               |GROUP BY ?id ?path ?name ?maybeDesc ?dateCreated ?maybeCreatorId ?maybeCreatorName ?visibility
               |LIMIT 1
               |""".stripMargin
    )
  )

  private implicit lazy val decoder: Decoder[Option[Project]] = ResultsDecoder[Option, Project] { implicit cur =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    def toListOfKeywords: Option[String] => Decoder.Result[List[projects.Keyword]] =
      _.map(_.split(',').toList.distinct.map(v => projects.Keyword.from(v)).sequence.map(_.sortBy(_.value))).sequence
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(_.getOrElse(List.empty))

    def toListOfImages(id: projects.ResourceId): Option[String] => Decoder.Result[List[Image]] =
      _.map(ImageUri.fromSplitString(','))
        .map(_.leftMap(ex => DecodingFailure(ex.getMessage, Nil)))
        .getOrElse(Nil.asRight)
        .map(Image.projectImage(id, _))

    for {
      id               <- extract[projects.ResourceId]("id")
      path             <- extract[projects.Path]("path")
      name             <- extract[projects.Name]("name")
      maybeDesc        <- extract[Option[projects.Description]]("maybeDesc")
      dateCreated      <- extract[projects.DateCreated]("dateCreated")
      maybeCreatorId   <- extract[Option[persons.ResourceId]]("maybeCreatorId")
      maybeCreatorName <- extract[Option[persons.Name]]("maybeCreatorName")
      visibility       <- extract[projects.Visibility]("visibility")
      keywords         <- extract[Option[String]]("keywords") >>= toListOfKeywords
      images           <- extract[Option[String]]("images") >>= toListOfImages(id)
    } yield NonRenkuProject.WithoutParent(
      id,
      path,
      name,
      maybeDesc,
      dateCreated,
      (maybeCreatorId -> maybeCreatorName).mapN(Person.WithNameOnly(_, _, None, None)),
      visibility,
      keywords.toSet,
      members = Set.empty,
      images
    )
  }
}

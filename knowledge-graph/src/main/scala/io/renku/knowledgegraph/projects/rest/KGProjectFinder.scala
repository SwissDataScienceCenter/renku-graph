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

package io.renku.knowledgegraph.projects.rest

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.config.RenkuBaseUrlLoader
import io.renku.graph.model.projects._
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{RenkuBaseUrl, SchemaVersion, users}
import io.renku.knowledgegraph.projects.rest.KGProjectFinder._
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait KGProjectFinder[F[_]] {
  def findProject(path: Path): F[Option[KGProject]]
}

private class KGProjectFinderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    renkuBaseUrl:   RenkuBaseUrl,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with KGProjectFinder[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._

  override def findProject(path: Path): F[Option[KGProject]] = {
    implicit val decoder: Decoder[List[KGProject]] = recordsDecoder(path)
    queryExpecting[List[KGProject]](using = query(path)) flatMap toSingleProject
  }

  private def query(path: Path) = SparqlQuery.of(
    name = "project by id",
    Prefixes.of(schema -> "schema", prov -> "prov", renku -> "renku"),
    s"""|SELECT DISTINCT ?name ?visibility ?maybeDescription ?dateCreated ?maybeCreatorName ?maybeCreatorEmail ?maybeParentId ?maybeParentName ?maybeParentDateCreated ?maybeParentCreatorName ?maybeParentCreatorEmail ?schemaVersion
        |WHERE {
        |  BIND (${ResourceId(path)(renkuBaseUrl).showAs[RdfResource]} AS ?projectId)
        |  ?projectId a schema:Project;
        |             schema:name ?name;
        |             renku:projectVisibility ?visibility;
        |             schema:schemaVersion ?schemaVersion;
        |             schema:dateCreated ?dateCreated.
        |  OPTIONAL {?projectId schema:description ?maybeDescription . }
        |  OPTIONAL {
        |    ?projectId schema:creator ?maybeCreatorId.
        |    ?maybeCreatorId a schema:Person;
        |                    schema:name ?maybeCreatorName.
        |    OPTIONAL { ?maybeCreatorId schema:email ?maybeCreatorEmail }
        |  }
        |  OPTIONAL {
        |    ?projectId prov:wasDerivedFrom ?maybeParentId.
        |    ?maybeParentId a schema:Project;
        |                   schema:name ?maybeParentName;
        |                   schema:dateCreated ?maybeParentDateCreated.
        |    OPTIONAL {
        |      ?maybeParentId schema:creator ?maybeParentCreatorId.
        |      ?maybeParentCreatorId a schema:Person;
        |                            schema:name ?maybeParentCreatorName.
        |      OPTIONAL { ?maybeParentCreatorId schema:email ?maybeParentCreatorEmail }
        |    }
        |  }
        |}
        |""".stripMargin
  )

  private def recordsDecoder(path: Path): Decoder[List[KGProject]] = {
    import Decoder._
    import io.renku.graph.model.projects._
    import io.renku.graph.model.users
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val project: Decoder[KGProject] = { cursor =>
      for {
        name                   <- cursor.downField("name").downField("value").as[Name]
        visibility             <- cursor.downField("visibility").downField("value").as[Visibility]
        dateCreated            <- cursor.downField("dateCreated").downField("value").as[DateCreated]
        maybeDescription       <- cursor.downField("maybeDescription").downField("value").as[Option[Description]]
        maybeCreatorName       <- cursor.downField("maybeCreatorName").downField("value").as[Option[users.Name]]
        maybeCreatorEmail      <- cursor.downField("maybeCreatorEmail").downField("value").as[Option[users.Email]]
        maybeParentId          <- cursor.downField("maybeParentId").downField("value").as[Option[ResourceId]]
        maybeParentName        <- cursor.downField("maybeParentName").downField("value").as[Option[Name]]
        maybeParentDateCreated <- cursor.downField("maybeParentDateCreated").downField("value").as[Option[DateCreated]]
        maybeParentCreatorName <- cursor.downField("maybeParentCreatorName").downField("value").as[Option[users.Name]]
        maybeParentCreatorEmail <- cursor
                                     .downField("maybeParentCreatorEmail")
                                     .downField("value")
                                     .as[Option[users.Email]]
        version <- cursor.downField("schemaVersion").downField("value").as[SchemaVersion]
      } yield KGProject(
        path,
        name,
        ProjectCreation(dateCreated, maybeCreatorName map (name => ProjectCreator(maybeCreatorEmail, name))),
        visibility,
        maybeParent =
          (maybeParentId, maybeParentName, maybeParentDateCreated) mapN { case (parentId, name, dateCreated) =>
            Parent(parentId,
                   name,
                   ProjectCreation(dateCreated,
                                   maybeParentCreatorName.map(name => ProjectCreator(maybeParentCreatorEmail, name))
                   )
            )
          },
        version,
        maybeDescription
      )
    }

    _.downField("results").downField("bindings").as(decodeList(project))
  }

  private lazy val toSingleProject: List[KGProject] => F[Option[KGProject]] = {
    case Nil            => Option.empty[KGProject].pure[F]
    case project +: Nil => project.some.pure[F]
    case projects =>
      new RuntimeException(s"More than one project with ${projects.head.path} path")
        .raiseError[F, Option[KGProject]]
  }
}

private object KGProjectFinder {

  final case class KGProject(path:             Path,
                             name:             Name,
                             created:          ProjectCreation,
                             visibility:       Visibility,
                             maybeParent:      Option[Parent],
                             version:          SchemaVersion,
                             maybeDescription: Option[Description]
  )

  final case class ProjectCreation(date: DateCreated, maybeCreator: Option[ProjectCreator])

  final case class Parent(resourceId: ResourceId, name: Name, created: ProjectCreation)

  final case class ProjectCreator(maybeEmail: Option[users.Email], name: users.Name)

  def apply[F[_]: Async: Logger](
      timeRecorder: SparqlQueryTimeRecorder[F]
  ): F[KGProjectFinder[F]] = for {
    config       <- RdfStoreConfig[F]()
    renkuBaseUrl <- RenkuBaseUrlLoader[F]()
  } yield new KGProjectFinderImpl(config, renkuBaseUrl, timeRecorder)
}

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

package io.renku.knowledgegraph.projects.rest

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects._
import io.renku.graph.model.{SchemaVersion, persons}
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.projects.rest.KGProjectFinder._
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait KGProjectFinder[F[_]] {
  def findProject(path: Path, maybeAuthUser: Option[AuthUser]): F[Option[KGProject]]
}

private class KGProjectFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig: RdfStoreConfig
) extends RdfStoreClientImpl(rdfStoreConfig)
    with KGProjectFinder[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._

  override def findProject(path: Path, maybeAuthUser: Option[AuthUser]): F[Option[KGProject]] = {
    implicit val decoder: Decoder[List[KGProject]] = recordsDecoder(path)
    queryExpecting[List[KGProject]](using = query(path, maybeAuthUser)) >>= toSingleProject
  }

  private def query(path: Path, maybeAuthUser: Option[AuthUser]) = SparqlQuery.of(
    name = "project by id",
    Prefixes.of(schema -> "schema", prov -> "prov", renku -> "renku"),
    s"""|SELECT ?name ?visibility ?maybeDescription ?dateCreated ?maybeCreatorName ?maybeCreatorEmail 
        |       ?maybeParentId ?maybeParentName ?maybeParentDateCreated ?maybeParentCreatorName ?maybeParentCreatorEmail 
        |       ?maybeSchemaVersion (GROUP_CONCAT(?keyword; separator=',') AS ?keywords)
        |WHERE {
        |  ?projectId a schema:Project;
        |             renku:projectPath '$path';
        |             schema:name ?name;
        |             renku:projectVisibility ?visibility;
        |             schema:dateCreated ?dateCreated.
        |  OPTIONAL { ?projectId schema:schemaVersion ?maybeSchemaVersion }
        |  OPTIONAL { ?projectId schema:description ?maybeDescription }
        |  OPTIONAL { ?projectId schema:keywords ?keyword }
        |  OPTIONAL {
        |    ?projectId schema:creator ?maybeCreatorId.
        |    ?maybeCreatorId a schema:Person;
        |                    schema:name ?maybeCreatorName.
        |    OPTIONAL { ?maybeCreatorId schema:email ?maybeCreatorEmail }
        |  }
        |  OPTIONAL {
        |    ?projectId prov:wasDerivedFrom ?maybeParentId.
        |    ${parentMemberFilterQuery(maybeAuthUser)}
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
        |GROUP BY ?name ?visibility ?maybeDescription ?dateCreated ?maybeCreatorName ?maybeCreatorEmail ?maybeParentId ?maybeParentName ?maybeParentDateCreated ?maybeParentCreatorName ?maybeParentCreatorEmail ?maybeSchemaVersion
        |""".stripMargin
  )

  private lazy val parentMemberFilterQuery: Option[AuthUser] => String = {
    case Some(user) =>
      s"""|?maybeParentId renku:projectVisibility ?parentVisibility .
          |OPTIONAL {
          |  ?maybeParentId schema:member/schema:sameAs ?memberId.
          |  ?memberId schema:additionalType 'GitLab';
          |            schema:identifier ?userGitlabId .
          |}
          |FILTER ( ?parentVisibility = '${Visibility.Public.value}' || ?userGitlabId = ${user.id.value} )
          |""".stripMargin
    case _ =>
      s"""|?maybeParentId renku:projectVisibility ?parentVisibility .
          |FILTER(?parentVisibility = '${Visibility.Public.value}')
          |""".stripMargin
  }

  private def recordsDecoder(path: Path): Decoder[List[KGProject]] = {
    import Decoder._
    import io.circe.DecodingFailure
    import io.renku.graph.model.projects._
    import io.renku.graph.model.persons
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val toSetOfKeywords: Option[String] => Decoder.Result[Set[Keyword]] =
      _.map(_.split(',').toList.map(Keyword.from).sequence.map(_.toSet)).sequence
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(_.getOrElse(Set.empty))

    val project: Decoder[KGProject] = { cursor =>
      for {
        name              <- cursor.downField("name").downField("value").as[Name]
        visibility        <- cursor.downField("visibility").downField("value").as[Visibility]
        dateCreated       <- cursor.downField("dateCreated").downField("value").as[DateCreated]
        maybeDescription  <- cursor.downField("maybeDescription").downField("value").as[Option[Description]]
        keywords          <- cursor.downField("keywords").downField("value").as[Option[String]].flatMap(toSetOfKeywords)
        maybeCreatorName  <- cursor.downField("maybeCreatorName").downField("value").as[Option[persons.Name]]
        maybeCreatorEmail <- cursor.downField("maybeCreatorEmail").downField("value").as[Option[persons.Email]]
        maybeParentId     <- cursor.downField("maybeParentId").downField("value").as[Option[ResourceId]]
        maybeParentName   <- cursor.downField("maybeParentName").downField("value").as[Option[Name]]
        maybeParentDateCreated <- cursor.downField("maybeParentDateCreated").downField("value").as[Option[DateCreated]]
        maybeParentCreatorName <- cursor.downField("maybeParentCreatorName").downField("value").as[Option[persons.Name]]
        maybeParentCreatorEmail <- cursor
                                     .downField("maybeParentCreatorEmail")
                                     .downField("value")
                                     .as[Option[persons.Email]]
        maybeVersion <- cursor.downField("maybeSchemaVersion").downField("value").as[Option[SchemaVersion]]
      } yield KGProject(
        path,
        name,
        ProjectCreation(dateCreated, maybeCreatorName map (name => ProjectCreator(maybeCreatorEmail, name))),
        visibility,
        maybeParent =
          (maybeParentId, maybeParentName, maybeParentDateCreated) mapN { case (parentId, name, dateCreated) =>
            KGParent(parentId,
                     name,
                     ProjectCreation(dateCreated,
                                     maybeParentCreatorName.map(name => ProjectCreator(maybeParentCreatorEmail, name))
                     )
            )
          },
        maybeVersion,
        maybeDescription,
        keywords
      )
    }

    _.downField("results").downField("bindings").as(decodeList(project))
  }

  private lazy val toSingleProject: List[KGProject] => F[Option[KGProject]] = {
    case Nil            => Option.empty[KGProject].pure[F]
    case project +: Nil => project.some.pure[F]
    case projects =>
      new RuntimeException(s"Multiple projects or values for ${projects.head.path}")
        .raiseError[F, Option[KGProject]]
  }
}

private object KGProjectFinder {

  final case class KGProject(path:             Path,
                             name:             Name,
                             created:          ProjectCreation,
                             visibility:       Visibility,
                             maybeParent:      Option[KGParent],
                             maybeVersion:     Option[SchemaVersion],
                             maybeDescription: Option[Description],
                             keywords:         Set[Keyword]
  )

  final case class ProjectCreation(date: DateCreated, maybeCreator: Option[ProjectCreator])

  final case class KGParent(resourceId: ResourceId, name: Name, created: ProjectCreation)

  final case class ProjectCreator(maybeEmail: Option[persons.Email], name: persons.Name)

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGProjectFinder[F]] = for {
    config <- RdfStoreConfig[F]()
  } yield new KGProjectFinderImpl(config)
}

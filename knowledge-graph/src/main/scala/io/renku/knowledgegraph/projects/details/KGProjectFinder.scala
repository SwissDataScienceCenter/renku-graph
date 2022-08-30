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

package io.renku.knowledgegraph.projects.details

import KGProjectFinder._
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects._
import io.renku.graph.model.{SchemaVersion, persons}
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait KGProjectFinder[F[_]] {
  def findProject(path: Path, maybeAuthUser: Option[AuthUser]): F[Option[KGProject]]
}

private class KGProjectFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    renkuConnectionConfig: RenkuConnectionConfig
) extends TSClientImpl(renkuConnectionConfig)
    with KGProjectFinder[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._

  override def findProject(path: Path, maybeAuthUser: Option[AuthUser]): F[Option[KGProject]] =
    queryExpecting[Option[KGProject]](using = query(path, maybeAuthUser))(recordsDecoder(path))

  private def query(path: Path, maybeAuthUser: Option[AuthUser]) = SparqlQuery.of(
    name = "project by id",
    Prefixes.of(schema -> "schema", prov -> "prov", renku -> "renku"),
    s"""|SELECT ?resourceId ?name ?visibility ?maybeDescription ?dateCreated 
        |       ?maybeCreatorResourceId ?maybeCreatorName ?maybeCreatorEmail ?maybeCreatorAffiliation
        |       ?maybeParentResourceId ?maybeParentPath ?maybeParentName ?maybeParentDateCreated 
        |       ?maybeParentCreatorResourceId ?maybeParentCreatorName ?maybeParentCreatorEmail ?maybeParentCreatorAffiliation
        |       ?maybeSchemaVersion (GROUP_CONCAT(?keyword; separator=',') AS ?keywords)
        |WHERE {
        |  ?resourceId a schema:Project;
        |              renku:projectPath '$path';
        |              schema:name ?name;
        |              renku:projectVisibility ?visibility;
        |              schema:dateCreated ?dateCreated.
        |  OPTIONAL { ?resourceId schema:schemaVersion ?maybeSchemaVersion }
        |  OPTIONAL { ?resourceId schema:description ?maybeDescription }
        |  OPTIONAL { ?resourceId schema:keywords ?keyword }
        |  OPTIONAL {
        |    ?resourceId schema:creator ?maybeCreatorResourceId.
        |    ?maybeCreatorResourceId a schema:Person;
        |                            schema:name ?maybeCreatorName.
        |    OPTIONAL { ?maybeCreatorResourceId schema:email ?maybeCreatorEmail }
        |    OPTIONAL { ?maybeCreatorResourceId schema:affiliation ?maybeCreatorAffiliation }
        |  }
        |  OPTIONAL {
        |    ?resourceId prov:wasDerivedFrom ?maybeParentResourceId.
        |    ${parentMemberFilterQuery(maybeAuthUser)}
        |    ?maybeParentResourceId a schema:Project;
        |                           renku:projectPath ?maybeParentPath;
        |                           schema:name ?maybeParentName;
        |                           schema:dateCreated ?maybeParentDateCreated.
        |    OPTIONAL {
        |      ?maybeParentResourceId schema:creator ?maybeParentCreatorResourceId.
        |      ?maybeParentCreatorResourceId a schema:Person;
        |                            schema:name ?maybeParentCreatorName.
        |      OPTIONAL { ?maybeParentCreatorResourceId schema:email ?maybeParentCreatorEmail }
        |      OPTIONAL { ?maybeParentCreatorResourceId schema:affiliation ?maybeParentCreatorAffiliation }
        |    }
        |  }
        |}
        |GROUP BY ?resourceId ?name ?visibility ?maybeDescription ?dateCreated
        |         ?maybeCreatorResourceId ?maybeCreatorName ?maybeCreatorEmail ?maybeCreatorAffiliation
        |         ?maybeParentResourceId ?maybeParentPath ?maybeParentName ?maybeParentDateCreated 
        |         ?maybeParentCreatorResourceId ?maybeParentCreatorName ?maybeParentCreatorEmail ?maybeParentCreatorAffiliation
        |         ?maybeSchemaVersion
        |""".stripMargin
  )

  private lazy val parentMemberFilterQuery: Option[AuthUser] => String = {
    case Some(user) =>
      s"""|?maybeParentResourceId renku:projectVisibility ?parentVisibility .
          |OPTIONAL {
          |  ?maybeParentResourceId schema:member/schema:sameAs ?memberId.
          |  ?memberId schema:additionalType 'GitLab';
          |            schema:identifier ?userGitlabId .
          |}
          |FILTER ( ?parentVisibility = '${Visibility.Public.value}' || ?userGitlabId = ${user.id.value} )
          |""".stripMargin
    case _ =>
      s"""|?maybeParentResourceId renku:projectVisibility ?parentVisibility .
          |FILTER(?parentVisibility = '${Visibility.Public.value}')
          |""".stripMargin
  }

  private def recordsDecoder(path: Path): Decoder[Option[KGProject]] = {
    import Decoder._
    import io.circe.DecodingFailure
    import io.renku.graph.model.persons
    import io.renku.graph.model.projects._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val toSetOfKeywords: Option[String] => Decoder.Result[Set[Keyword]] =
      _.map(_.split(',').toList.map(Keyword.from).sequence.map(_.toSet)).sequence
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(_.getOrElse(Set.empty))

    ResultsDecoder[Option, KGProject] { implicit cur =>
      for {
        resourceId                    <- extract[ResourceId]("resourceId")
        name                          <- extract[Name]("name")
        visibility                    <- extract[Visibility]("visibility")
        dateCreated                   <- extract[DateCreated]("dateCreated")
        maybeDescription              <- extract[Option[Description]]("maybeDescription")
        keywords                      <- extract[Option[String]]("keywords").flatMap(toSetOfKeywords)
        maybeCreatorResourceId        <- extract[Option[persons.ResourceId]]("maybeCreatorResourceId")
        maybeCreatorName              <- extract[Option[persons.Name]]("maybeCreatorName")
        maybeCreatorEmail             <- extract[Option[persons.Email]]("maybeCreatorEmail")
        maybeCreatorAffiliation       <- extract[Option[persons.Affiliation]]("maybeCreatorAffiliation")
        maybeParentResourceId         <- extract[Option[ResourceId]]("maybeParentResourceId")
        maybeParentPath               <- extract[Option[Path]]("maybeParentPath")
        maybeParentName               <- extract[Option[Name]]("maybeParentName")
        maybeParentDateCreated        <- extract[Option[DateCreated]]("maybeParentDateCreated")
        maybeParentCreatorResourceId  <- extract[Option[persons.ResourceId]]("maybeParentCreatorResourceId")
        maybeParentCreatorName        <- extract[Option[persons.Name]]("maybeParentCreatorName")
        maybeParentCreatorEmail       <- extract[Option[persons.Email]]("maybeParentCreatorEmail")
        maybeParentCreatorAffiliation <- extract[Option[persons.Affiliation]]("maybeParentCreatorAffiliation")
        maybeVersion                  <- extract[Option[SchemaVersion]]("maybeSchemaVersion")
      } yield KGProject(
        resourceId,
        path,
        name,
        ProjectCreation(dateCreated,
                        (maybeCreatorResourceId, maybeCreatorName).mapN { case (id, name) =>
                          ProjectCreator(id, name, maybeCreatorEmail, maybeCreatorAffiliation)
                        }
        ),
        visibility,
        maybeParent = (maybeParentResourceId, maybeParentPath, maybeParentName, maybeParentDateCreated) mapN {
          case (parentId, path, name, dateCreated) =>
            KGParent(
              parentId,
              path,
              name,
              ProjectCreation(
                dateCreated,
                (maybeParentCreatorResourceId, maybeParentCreatorName).mapN { case (id, name) =>
                  ProjectCreator(id, name, maybeParentCreatorEmail, maybeParentCreatorAffiliation)
                }
              )
            )
        },
        maybeVersion,
        maybeDescription,
        keywords
      )
    }(toOption(show"Multiple projects or values for $path"))
  }
}

private object KGProjectFinder {

  final case class KGProject(resourceId:       ResourceId,
                             path:             Path,
                             name:             Name,
                             created:          ProjectCreation,
                             visibility:       Visibility,
                             maybeParent:      Option[KGParent],
                             maybeVersion:     Option[SchemaVersion],
                             maybeDescription: Option[Description],
                             keywords:         Set[Keyword]
  )

  final case class ProjectCreation(date: DateCreated, maybeCreator: Option[ProjectCreator])

  final case class KGParent(resourceId: ResourceId, path: Path, name: Name, created: ProjectCreation)

  final case class ProjectCreator(resourceId:       persons.ResourceId,
                                  name:             persons.Name,
                                  maybeEmail:       Option[persons.Email],
                                  maybeAffiliation: Option[persons.Affiliation]
  )

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGProjectFinder[F]] =
    RenkuConnectionConfig[F]().map(new KGProjectFinderImpl(_))
}
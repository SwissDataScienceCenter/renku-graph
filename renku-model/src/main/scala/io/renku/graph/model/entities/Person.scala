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

package io.renku.graph.model.entities

import cats.data.ValidatedNel
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.persons.{Affiliation, Email, GitLabId, Name, ResourceId}
import io.renku.graph.model.{GitLabApiUrl, RenkuBaseUrl}
import io.renku.jsonld._

sealed trait Person extends PersonAlgebra with Product with Serializable {
  type Id <: ResourceId
  val resourceId:       Id
  val name:             Name
  val maybeAffiliation: Option[Affiliation]
  val maybeEmail:       Option[Email]
}

sealed trait PersonAlgebra {
  def add(gitLabId: GitLabId)(implicit renkuBaseUrl: RenkuBaseUrl): Person.WithGitLabId
}

object Person {

  final case class WithGitLabId(
      resourceId:       ResourceId.GitLabIdBased,
      gitLabId:         GitLabId,
      name:             Name,
      maybeEmail:       Option[Email],
      maybeAffiliation: Option[Affiliation]
  ) extends Person {

    override type Id = ResourceId.GitLabIdBased

    override def add(gitLabId: GitLabId)(implicit renkuBaseUrl: RenkuBaseUrl): WithGitLabId =
      copy(resourceId = ResourceId(gitLabId), gitLabId = gitLabId)
  }

  final case class WithEmail(
      resourceId:       ResourceId.EmailBased,
      name:             Name,
      email:            Email,
      maybeAffiliation: Option[Affiliation]
  ) extends Person {
    override type Id = ResourceId.EmailBased

    val maybeEmail: Option[Email] = Some(email)

    override def add(gitLabId: GitLabId)(implicit renkuBaseUrl: RenkuBaseUrl): WithGitLabId =
      Person.WithGitLabId(ResourceId(gitLabId), gitLabId, name, email.some, maybeAffiliation)
  }

  final case class WithNameOnly(
      resourceId:       ResourceId.NameBased,
      name:             Name,
      maybeAffiliation: Option[Affiliation]
  ) extends Person {
    override type Id = ResourceId.NameBased

    val maybeEmail: Option[Email] = None

    override def add(gitLabId: GitLabId)(implicit renkuBaseUrl: RenkuBaseUrl): WithGitLabId =
      Person.WithGitLabId(ResourceId(gitLabId), gitLabId, name, maybeEmail = None, maybeAffiliation)
  }

  def from(
      resourceId:       ResourceId,
      name:             Name,
      maybeEmail:       Option[Email] = None,
      maybeAffiliation: Option[Affiliation] = None,
      maybeGitLabId:    Option[GitLabId] = None
  ): ValidatedNel[String, Person] = (resourceId, maybeGitLabId, maybeEmail) match {
    case (id: ResourceId.GitLabIdBased, Some(gitLabId), maybeEmail) =>
      Person.WithGitLabId(id, gitLabId, name, maybeEmail, maybeAffiliation).validNel
    case (id: ResourceId.EmailBased, None, Some(email)) =>
      Person.WithEmail(id, name, email, maybeAffiliation).validNel
    case (id: ResourceId.NameBased, None, None) =>
      Person.WithNameOnly(id, name, maybeAffiliation).validNel
    case _ => show"Invalid Person with $resourceId, gitLabId = $maybeGitLabId, email = $maybeEmail".invalidNel
  }

  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.JsonLDDecoder.decodeOption
  import io.renku.jsonld.JsonLDEncoder.encodeOption
  import io.renku.jsonld.syntax._

  val entityTypes: EntityTypes = EntityTypes.of(prov / "Person", schema / "Person")

  implicit def encoder[P <: Person](implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[P] =
    JsonLDEncoder.instance {
      case Person.WithGitLabId(id, gitLabId, name, maybeEmail, maybeAffiliation) =>
        JsonLD.entity(
          id.asEntityId,
          entityTypes,
          schema / "email"       -> maybeEmail.asJsonLD,
          schema / "name"        -> name.asJsonLD,
          schema / "affiliation" -> maybeAffiliation.asJsonLD,
          schema / "sameAs"      -> gitLabId.asJsonLD(gitLabIdEncoder)
        )
      case Person.WithEmail(id, name, email, maybeAffiliation) =>
        JsonLD.entity(
          id.asEntityId,
          entityTypes,
          schema / "email"       -> email.asJsonLD,
          schema / "name"        -> name.asJsonLD,
          schema / "affiliation" -> maybeAffiliation.asJsonLD
        )
      case Person.WithNameOnly(id, name, maybeAffiliation) =>
        JsonLD.entity(
          id.asEntityId,
          entityTypes,
          schema / "name"        -> name.asJsonLD,
          schema / "affiliation" -> maybeAffiliation.asJsonLD
        )
    }

  private val gitLabSameAsTypes:          EntityTypes = EntityTypes.of(schema / "URL")
  private val gitLabSameAsAdditionalType: String      = "GitLab"

  def toSameAsEntityId(gitLabId: GitLabId)(implicit gitLabApiUrl: GitLabApiUrl): EntityId =
    EntityId of (gitLabApiUrl / "users" / gitLabId).show

  def gitLabIdEncoder(implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[GitLabId] = JsonLDEncoder.instance {
    gitLabId =>
      JsonLD.entity(
        toSameAsEntityId(gitLabId),
        gitLabSameAsTypes,
        schema / "identifier"     -> gitLabId.value.asJsonLD,
        schema / "additionalType" -> gitLabSameAsAdditionalType.asJsonLD
      )
  }

  implicit lazy val decoder: JsonLDDecoder[Person] = JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
    import io.renku.graph.model.views.StringTinyTypeJsonLDDecoders._
    for {
      resourceId       <- cursor.downEntityId.as[ResourceId]
      names            <- cursor.downField(schema / "name").as[List[Name]]
      maybeEmail       <- cursor.downField(schema / "email").as[Option[Email]]
      maybeGitLabId    <- cursor.downField(schema / "sameAs").as[Option[GitLabId]](decodeOption(gitLabIdDecoder))
      maybeAffiliation <- cursor.downField(schema / "affiliation").as[List[Affiliation]].map(_.reverse.headOption)
      name <- if (names.isEmpty) DecodingFailure(s"No name on Person $resourceId", Nil).asLeft
              else names.reverse.head.asRight
      person <- Person
                  .from(resourceId, name, maybeEmail, maybeAffiliation, maybeGitLabId)
                  .toEither
                  .leftMap(errs => DecodingFailure(errs.nonEmptyIntercalate("; "), Nil))
    } yield person
  }

  private lazy val gitLabIdDecoder: JsonLDDecoder[GitLabId] = JsonLDDecoder.entity(gitLabSameAsTypes) { cursor =>
    for {
      _ <- cursor.downField(schema / "additionalType").as[String].flatMap {
             case `gitLabSameAsAdditionalType` => ().asRight
             case additionalType => DecodingFailure(s"Unknown $additionalType type of Person SameAs", Nil).asLeft
           }
      gitLabId <- cursor.downField(schema / "identifier").as[GitLabId]
    } yield gitLabId
  }
}

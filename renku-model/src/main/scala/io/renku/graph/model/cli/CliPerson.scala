package io.renku.graph.model.cli

import cats.syntax.either._
import io.circe.DecodingFailure
import io.renku.graph.model.Schemas.{prov, schema}
import io.renku.graph.model.persons.{Affiliation, Email, Name, ResourceId}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliPerson(
    resourceId:  ResourceId,
    name:        Name,
    email:       Option[Email],
    affiliation: Option[Affiliation]
)

object CliPerson {

  private val entityTypes: EntityTypes = EntityTypes.of(schema / "Person", prov / "Person")

  implicit def jsonLDDecoder: JsonLDDecoder[CliPerson] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        names            <- cursor.downField(schema / "name").as[List[Name]]
        maybeEmail       <- cursor.downField(schema / "email").as[Option[Email]]
        maybeAffiliation <- cursor.downField(schema / "affiliation").as[List[Affiliation]].map(_.reverse.headOption)
        name <- if (names.isEmpty) DecodingFailure(s"No name on Person $resourceId", Nil).asLeft
                else names.reverse.head.asRight
        person =
          CliPerson(resourceId, name, maybeEmail, maybeAffiliation)
      } yield person
    }

  implicit def jsonLDEncoder: JsonLDEncoder[CliPerson] =
    JsonLDEncoder.instance { person =>
      JsonLD.entity(
        person.resourceId.asEntityId,
        entityTypes,
        List(
          Some(schema / "name" -> person.name.asJsonLD),
          person.email.map(m => schema / "email" -> m.asJsonLD),
          person.affiliation.map(a => schema / "affiliation" -> a.asJsonLD)
        ).flatten.toMap
      )
    }
}

package io.renku.cli.model

import cats.syntax.either._
import io.circe.DecodingFailure
import io.renku.cli.model.Ontologies.{Prov, Schema}
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

  private val entityTypes: EntityTypes = EntityTypes.of(Schema.Person, Prov.Person)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  implicit def jsonLDDecoder: JsonLDDecoder[CliPerson] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        names            <- cursor.downField(Schema.name).as[List[Name]]
        maybeEmail       <- cursor.downField(Schema.email).as[Option[Email]]
        maybeAffiliation <- cursor.downField(Schema.affiliation).as[List[Affiliation]].map(_.reverse.headOption)
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
          Some(Schema.name -> person.name.asJsonLD),
          person.email.map(m => Schema.email -> m.asJsonLD),
          person.affiliation.map(a => Schema.affiliation -> a.asJsonLD)
        ).flatten.toMap
      )
    }
}

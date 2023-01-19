package io.renku.cli.model

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.CliCollection.Member
import io.renku.cli.model.Ontologies.{Prov, Renku}
import io.renku.graph.model.entityModel._
import io.renku.jsonld.JsonLDDecoder.Result
import io.renku.jsonld.syntax._
import io.renku.jsonld._

final case class CliCollection(
    resourceId: ResourceId,
    path:       EntityPath,
    checksum:   Checksum,
    members:    List[Member]
)

object CliCollection {

  sealed trait Member {
    def fold[A](fa: CliEntity => A, fb: CliCollection => A): A
  }

  object Member {
    final case class Entity(entity: CliEntity) extends Member {
      def fold[A](fa: CliEntity => A, fb: CliCollection => A): A = fa(entity)
    }
    final case class Collection(collection: CliCollection) extends Member {
      def fold[A](fa: CliEntity => A, fb: CliCollection => A): A = fb(collection)
    }

    def apply(entity: CliEntity):     Member = Entity(entity)
    def apply(coll:   CliCollection): Member = Collection(coll)

    implicit def jsonLDDecoder: JsonLDDecoder[Member] = {
      val da = CliEntity.jsonLdDecoder.emap(e => Right(Member(e)))
      val db = CliCollection.jsonLdDecoder.emap(e => Right(Member(e)))

      JsonLDDecoder.instance { cursor =>
        val currentTypes = cursor.getEntityTypes
        (currentTypes.map(CliEntity.matchingEntityTypes), currentTypes.map(CliCollection.matchingEntityTypes))
          .flatMapN {
            case (true, _) => da(cursor)
            case (_, true) => db(cursor)
            case _ => Left(DecodingFailure(s"Invalid entity types for decoding member entity: $entityTypes", Nil))
          }
      }
    }

    implicit def jsonLDEncoder: JsonLDEncoder[Member] =
      JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))
  }

  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Entity, Prov.Collection)

  private[model] def matchingEntityTypes(entityTypes: EntityTypes): Boolean =
    entityTypes == this.entityTypes

  private val withStrictEntityTypes: Cursor => Result[Boolean] =
    _.getEntityTypes.map(types => types == entityTypes)

  implicit def jsonLdDecoder: JsonLDDecoder[CliCollection] =
    JsonLDDecoder.cacheableEntity(entityTypes, withStrictEntityTypes) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        path       <- cursor.downField(Prov.atLocation).as[EntityPath]
        checksum   <- cursor.downField(Renku.checksum).as[Checksum]
        members    <- cursor.downField(Prov.hadMember).as[List[Member]]
      } yield CliCollection(resourceId, path, checksum, members)
    }

  implicit def jsonLDEncoder: FlatJsonLDEncoder[CliCollection] =
    FlatJsonLDEncoder.unsafe { entity =>
      JsonLD.entity(
        entity.resourceId.asEntityId,
        entityTypes,
        Prov.atLocation -> entity.path.asJsonLD,
        Renku.checksum  -> entity.checksum.asJsonLD,
        Prov.hadMember  -> entity.members.asJsonLD
      )
    }
}

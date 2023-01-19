package io.renku.cli.model

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.CliGeneration.QualifiedGeneration
import io.renku.cli.model.Ontologies.Prov
import io.renku.graph.model.generations._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder, Property, Reverse}

final case class CliGeneration(
    resourceId: ResourceId,
    entity:     QualifiedGeneration
)

object CliGeneration {

  sealed trait QualifiedGeneration {
    def fold[A](fa: CliEntity => A, fb: CliCollection => A): A
  }

  object QualifiedGeneration {
    final case class Entity(entity: CliEntity) extends QualifiedGeneration {
      def fold[A](fa: CliEntity => A, fb: CliCollection => A): A = fa(entity)
    }

    final case class Collection(collection: CliCollection) extends QualifiedGeneration {
      def fold[A](fa: CliEntity => A, fb: CliCollection => A): A = fb(collection)
    }

    def apply(entity: CliEntity):     QualifiedGeneration = Entity(entity)
    def apply(coll:   CliCollection): QualifiedGeneration = Collection(coll)

    implicit def jsonLDDecoder: JsonLDDecoder[QualifiedGeneration] = {
      val da = CliEntity.jsonLdDecoder.emap(e => Right(QualifiedGeneration(e)))
      val db = CliCollection.jsonLdDecoder.emap(e => Right(QualifiedGeneration(e)))
      JsonLDDecoder.instance { cursor =>
        val currentTypes = cursor.getEntityTypes
        (currentTypes.map(CliEntity.matchingEntityTypes), currentTypes.map(CliCollection.matchingEntityTypes))
          .flatMapN {
            case (true, _) => da(cursor)
            case (_, true) => db(cursor)
            case _ => Left(DecodingFailure(s"Invalid entity types for decoding generation entity: $entityTypes", Nil))
          }
      }
    }

    implicit def jsonLDEncoder: JsonLDEncoder[QualifiedGeneration] =
      JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))
  }

  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Generation)

  implicit lazy val jsonLDEncoder: FlatJsonLDEncoder[CliGeneration] =
    FlatJsonLDEncoder.unsafe { generation =>
      JsonLD.entity(
        generation.resourceId.asEntityId,
        entityTypes,
        Reverse.ofJsonLDsUnsafe(Prov.qualifiedGeneration -> generation.entity.asJsonLD),
        Map.empty[Property, JsonLD]
      )
    }

  implicit val jsonLDDecoder: JsonLDDecoder[CliGeneration] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        entity <- cursor.focusTop
                    .as[List[QualifiedGeneration]]
        // .downField(Prov.qualifiedGeneration).as[QualifiedGeneration]
      } yield CliGeneration(resourceId, entity.head)
    }

}

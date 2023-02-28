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

package io.renku.cli.model

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.Ontologies.Prov
import io.renku.graph.model.{entityModel, generations}
import io.renku.jsonld.JsonLDDecoder.Result
import io.renku.jsonld.syntax._
import io.renku.jsonld.{Cursor, EntityTypes, JsonLDDecoder, JsonLDEncoder}
import monocle.Lens

sealed trait CliEntity extends CliModel {
  def resourceId:    entityModel.ResourceId
  def path:          EntityPath
  def checksum:      entityModel.Checksum
  def generationIds: List[generations.ResourceId]

  def fold[A](fa: CliSingleEntity => A, fb: CliCollectionEntity => A): A
}

object CliEntity {

  final case class Entity(entity: CliSingleEntity) extends CliEntity {
    override val resourceId:    entityModel.ResourceId       = entity.resourceId
    override val generationIds: List[generations.ResourceId] = entity.generationIds
    override val path:          EntityPath                   = entity.path
    override val checksum:      entityModel.Checksum         = entity.checksum

    def fold[A](fa: CliSingleEntity => A, fb: CliCollectionEntity => A): A = fa(entity)
  }

  final case class Collection(collection: CliCollectionEntity) extends CliEntity {
    override val resourceId:    entityModel.ResourceId       = collection.resourceId
    override val generationIds: List[generations.ResourceId] = collection.generationIds
    override val path:          EntityPath                   = collection.path
    override val checksum:      entityModel.Checksum         = collection.checksum

    def fold[A](fa: CliSingleEntity => A, fb: CliCollectionEntity => A): A = fb(collection)
  }

  def apply(entity: CliSingleEntity): CliEntity = Entity(entity)

  def apply(coll: CliCollectionEntity): CliEntity = Collection(coll)

  private val entityTypes = EntityTypes.of(Prov.Entity)

  private def selectCandidates(ets: EntityTypes): Boolean =
    CliSingleEntity.matchingEntityTypes(ets) || CliCollectionEntity.matchingEntityTypes(ets)

  implicit def jsonLDDecoder: JsonLDDecoder[CliEntity] =
    jsonLDDecoderFor(_ => Right(true))

  def jsonLDDecoderForGeneration(generationId: generations.ResourceId): JsonLDDecoder[CliEntity] =
    jsonLDDecoderFor(withSpecificGeneration(generationId))

  private def jsonLDDecoderFor(filter: Cursor => Result[Boolean]): JsonLDDecoder[CliEntity] = {
    val da = CliSingleEntity.jsonLDDecoder.emap(e => Right(CliEntity(e)))
    val db = CliCollectionEntity.jsonLdDecoder.emap(e => Right(CliEntity(e)))
    JsonLDDecoder.entity(entityTypes, c => (c.getEntityTypes.map(selectCandidates), filter(c)).mapN(_ && _)) { cursor =>
      val currentTypes = cursor.getEntityTypes
      (currentTypes.map(CliSingleEntity.matchingEntityTypes), currentTypes.map(CliCollectionEntity.matchingEntityTypes))
        .flatMapN {
          case (_, true) => db(cursor)
          case (true, _) => da(cursor)
          case _ =>
            Left(
              DecodingFailure(s"Invalid entity types for decoding related entity for a generation: $entityTypes", Nil)
            )
        }
    }
  }

  private def withSpecificGeneration(generationId: generations.ResourceId): Cursor => Result[Boolean] =
    _.downField(Prov.qualifiedGeneration)
      .as[List[generations.ResourceId]]
      .map(_.contains(generationId))

  implicit def jsonLDEncoder: JsonLDEncoder[CliEntity] = JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))

  object Lenses {
    val entityPath: Lens[CliEntity, EntityPath] =
      Lens[CliEntity, EntityPath](_.path)(path => {
        case Entity(e)     => CliEntity(e.copy(path = path))
        case Collection(e) => CliEntity(e.copy(path = path))
      })

    val generationIds: Lens[CliEntity, List[generations.ResourceId]] =
      Lens[CliEntity, List[generations.ResourceId]](_.generationIds)(ids =>
        _.fold(e => CliEntity(e.copy(generationIds = ids)), e => CliEntity(e.copy(generationIds = ids)))
      )
  }
}

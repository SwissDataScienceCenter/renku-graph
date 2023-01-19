package io.renku.cli.model

import io.renku.jsonld.JsonLD.{JsonLDArray, JsonLDEntity}
import io.renku.jsonld.JsonLDDecoder.Result
import io.renku.jsonld.{Cursor, EntityId, JsonLD, JsonLDDecoder, Property}

trait JsonDecoderSupport {

  final implicit class CursorOps(self: Cursor) {
    def findEntity(id: EntityId): Option[JsonLD] =
      self.focusTop.jsonLD match {
        case array: JsonLDArray =>
          array.jsons.find {
            case entity: JsonLDEntity => entity.id == id
            case _ => false
          }

        case _ => None
      }

    def decodeLinked[A: JsonLDDecoder](prop: Property)(idf: A => EntityId): Result[List[A]] = for {
      id    <- self.downField(prop).as[EntityId]
      other <- self.focusTop.as[List[A]].map(_.filter(e => idf(e) == id))
    } yield other
  }
}

object JsonDecoderSupport extends JsonDecoderSupport

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import cats.implicits._
import ch.datascience.rdfstore.JsonLDTriples
import io.circe.Decoder.decodeString
import io.circe.optics.JsonPath.root
import io.circe.{Decoder, Encoder, Json}
import io.renku.jsonld.EntityId
import monocle.function.Plated
import io.circe.optics.JsonOptics._

import scala.collection.mutable

private[triplescuration] class DataSetInfoFinder {

  def findEntityId(triples: JsonLDTriples): Set[EntityId] = {
    val collected = mutable.HashSet.empty[EntityId]
    Plated.transform[Json] { json =>
      root.`@type`.each.string.getAll(json) match {
        case types if types.contains("http://schema.org/Dataset") =>
          json.get[EntityId]("@id").map(collected.add)
          json
        case _ => json
      }
    }(triples.value)
    collected.toSet
  }

  private implicit class JsonOps(json: Json) {

    def get[T](property: String)(implicit decode: Decoder[T], encode: Encoder[T]): Option[T] =
      root.selectDynamic(property).as[T].getOption(json)
  }

  private implicit val entityIdDecoder: Decoder[EntityId] =
    decodeString.emap { value =>
      if (value.trim.isEmpty) "Empty entityId found in the generated triples".asLeft[EntityId]
      else EntityId.of(value).asRight[String]
    }
}

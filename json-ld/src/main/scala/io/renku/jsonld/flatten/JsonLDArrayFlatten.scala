package io.renku.jsonld.flatten

import io.renku.jsonld.JsonLD
import io.renku.jsonld.JsonLD.{JsonLDArray, JsonLDEntity, MalformedJsonLD}
import cats.syntax.all._

trait JsonLDArrayFlatten extends Flatten {
  self: JsonLDArray =>
  lazy val flatten: Either[MalformedJsonLD, JsonLD] =
    for {
      flattenedJsons <- this.jsons
                          .foldLeft(Either.right[MalformedJsonLD, List[JsonLD]](List.empty[JsonLD])) {
                            case (acc, jsonLDEntity: JsonLDEntity) =>
                              for {
                                jsons    <- deNest(List(jsonLDEntity), List.empty[JsonLDEntity])
                                accRight <- acc
                              } yield accRight ++ jsons
                            case (acc, other) => acc.map(other +: _)
                          }
      flattenedArray <- checkForUniqueIds(flattenedJsons.distinct)
    } yield flattenedArray
}

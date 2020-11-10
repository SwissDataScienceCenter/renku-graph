package io.renku.jsonld.flatten

import io.renku.jsonld.JsonLD
import io.renku.jsonld.JsonLD.{JsonLDEntity, MalformedJsonLD}

trait IDValidation {
  protected[flatten] def checkForUniqueIds(flattenedJsons: List[JsonLD]): Either[MalformedJsonLD, JsonLD] = if (
    areIdsUnique(flattenedJsons)
  )
    Right(JsonLD.arr(flattenedJsons: _*))
  else
    Left(MalformedJsonLD("Some entities share an ID even though they're not the same"))

  private def areIdsUnique(jsons: List[JsonLD]): Boolean =
    jsons
      .collect { case entity: JsonLDEntity => entity }
      .groupBy(entity => entity.id)
      .forall { case (_, entitiesPerId) =>
        entitiesPerId.forall(_ == entitiesPerId.head)
      }
}

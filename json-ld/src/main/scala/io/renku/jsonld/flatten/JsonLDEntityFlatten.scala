package io.renku.jsonld.flatten

import io.renku.jsonld.JsonLD
import io.renku.jsonld.JsonLD.{JsonLDEntity, MalformedJsonLD}

trait JsonLDEntityFlatten extends Flatten {
  self: JsonLDEntity =>

  lazy val flatten: Either[MalformedJsonLD, JsonLD] = deNest(List(this), Nil).flatMap(checkForUniqueIds)
}

package io.renku.triplesstore.model

import io.renku.jsonld.{EntityId, Property}

final case class Triple(subject: EntityId, predicate: Property, obj: TripleObject)

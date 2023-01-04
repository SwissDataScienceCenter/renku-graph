package io.renku.entities.searchgraphs.commands

import io.renku.graph.model.GraphClass
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityIdEncoder, Property}
import io.renku.triplesstore.syntax._
import io.renku.triplesstore.model.{Quad, TripleObjectEncoder}

private object DatasetsQuad {
  def apply[ID, O](subject: ID, predicate: Property, obj: O)(implicit
      subjectEncoder:       EntityIdEncoder[ID],
      objEncoder:           TripleObjectEncoder[O]
  ): Quad = Quad(GraphClass.Datasets.id, subject.asEntityId, predicate, obj.asTripleObject)
}

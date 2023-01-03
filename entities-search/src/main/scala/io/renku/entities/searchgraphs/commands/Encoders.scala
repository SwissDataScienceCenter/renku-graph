package io.renku.entities.searchgraphs.commands

import io.renku.entities.searchgraphs.{Link, PersonInfo}
import io.renku.graph.model.Schemas.{rdf, renku}
import io.renku.graph.model.entities.Person
import io.renku.jsonld.syntax._
import io.renku.triplesstore.model.QuadsEncoder
import io.renku.triplesstore.model.syntax._

private object Encoders {

  implicit val personInfoEncoder: QuadsEncoder[PersonInfo] = QuadsEncoder.instance {
    case PersonInfo(resourceId, name) =>
      List(
        DatasetsQuad(resourceId, rdf / "type", Person.Ontology.typeClass.id),
        DatasetsQuad(resourceId, Person.Ontology.name, name)
      )
  }

  implicit val linkEncoder: QuadsEncoder[Link] = QuadsEncoder.instance { case Link(resourceId, dataset, project) =>
    List(
      DatasetsQuad(resourceId, rdf / "type", renku / "DatasetProjectLink"),
      DatasetsQuad(resourceId, renku / "project", project.asEntityId),
      DatasetsQuad(resourceId, renku / "dataset", dataset.asEntityId)
    )
  }
}

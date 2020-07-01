package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import ch.datascience.graph.model.datasets.{IdSameAs, SameAs}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.tinytypes.Renderer
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.Update
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId

private[triplescuration] class UpdatesCreator {

  def prepareUpdates(entityId: EntityId, topmostSameAs: IdSameAs): List[Update] = List(
    Update(
      s"Updating Dataset $entityId topmostSameAs",
      SparqlQuery(
        "upload - topmostSameAs update",
        Set(
          "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
        ),
        s"""|DELETE { ${entityId.asRdfResource} renku:topmostSameAs ?topmostSameAs }
            |INSERT { ${entityId.asRdfResource} renku:topmostSameAs ${topmostSameAs.showAs[RdfResource]} }
            |WHERE {
            |  OPTIONAL { ${entityId.asRdfResource} renku:topmostSameAs ?maybeSameAs }
            |  BIND (IF(BOUND(?maybeSameAs), ?maybeSameAs, "nonexisting") AS ?topmostSameAs)
            |}
            |""".stripMargin
      )
    )
  )

  private implicit class EntityIdOps(entityId: EntityId) {
    lazy val asRdfResource: String = s"<$entityId>"
  }

  private implicit val sameAsRdfRenderer: Renderer[RdfResource, SameAs] =
    (value: SameAs) => s"<$value>"
}

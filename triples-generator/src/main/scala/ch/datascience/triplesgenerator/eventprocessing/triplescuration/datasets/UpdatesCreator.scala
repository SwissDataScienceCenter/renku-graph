package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import ch.datascience.graph.model.datasets.{DerivedFrom, IdSameAs, SameAs}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.tinytypes.Renderer
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.Update
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId

private[triplescuration] class UpdatesCreator {

  def prepareUpdates(entityId: EntityId, topmostSameAs: IdSameAs, topmostDerivedFrom: DerivedFrom): List[Update] =
    prepareSameAsUpdates(entityId, topmostSameAs) ++: prepareDerivedFromUpdates(entityId, topmostDerivedFrom)

  private def prepareSameAsUpdates(entityId: EntityId, topmostSameAs: IdSameAs): List[Update] = List(
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

  private def prepareDerivedFromUpdates(entityId: EntityId, topmostDerivedFrom: DerivedFrom): List[Update] = List(
    Update(
      s"Updating Dataset $entityId topmostDerivedFrom",
      SparqlQuery(
        "upload - topmostDerivedFrom update",
        Set(
          "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
        ),
        s"""|DELETE { ${entityId.asRdfResource} renku:topmostDerivedFrom ?topmostDerivedFrom }
            |INSERT { ${entityId.asRdfResource} renku:topmostDerivedFrom ${topmostDerivedFrom.showAs[RdfResource]} }
            |WHERE {
            |  OPTIONAL { ${entityId.asRdfResource} renku:topmostDerivedFrom ?maybeDerivedFrom }
            |  BIND (IF(BOUND(?maybeDerivedFrom), ?maybeDerivedFrom, "nonexisting") AS ?topmostDerivedFrom)
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

  private implicit val derivedFromRdfRenderer: Renderer[RdfResource, DerivedFrom] =
    (value: DerivedFrom) => s"<$value>"
}

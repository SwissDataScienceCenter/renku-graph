package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs}
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.Update
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.TopmostDataFinder.TopmostData
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId

private class DescendantsUpdater {

  def prepareUpdates(curatedTriples: CuratedTriples, topmostData: TopmostData): CuratedTriples = curatedTriples.copy(
    updates =
      curatedTriples.updates ++: List(
        prepareSameAsUpdate(topmostData.datasetId, topmostData.sameAs),
        prepareDerivedFromUpdate(topmostData.datasetId, topmostData.derivedFrom)
      )
  )

  private def prepareSameAsUpdate(entityId: EntityId, topmostSameAs: SameAs) = Update(
    s"Updating Dataset $entityId topmostSameAs",
    SparqlQuery(
      "upload - topmostSameAs update",
      Set(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
        "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
        "PREFIX schema: <http://schema.org/>"
      ),
      s"""|DELETE { ?sameAs schema:url <$entityId> }
          |INSERT { ?sameAs schema:url <$topmostSameAs> }
          |WHERE {
          |  ?dsId rdf:type schema:Dataset;
          |        renku:topmostSameAs ?sameAs.
          |  ?sameAs schema:url <$entityId>
          |}
          |""".stripMargin
    )
  )

  private def prepareDerivedFromUpdate(entityId: EntityId, topmostDerivedFrom: DerivedFrom) = Update(
    s"Updating Dataset $entityId topmostDerivedFrom",
    SparqlQuery(
      "upload - topmostDerivedFrom update",
      Set(
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
        "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
        "PREFIX schema: <http://schema.org/>"
      ),
      s"""|DELETE { ?dsId renku:topmostDerivedFrom <$entityId> }
          |INSERT { ?dsId renku:topmostDerivedFrom <$topmostDerivedFrom> }
          |WHERE {
          |  ?dsId rdf:type schema:Dataset;
          |        renku:topmostDerivedFrom <$entityId>
          |}
          |""".stripMargin
    )
  )
}

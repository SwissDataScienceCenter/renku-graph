package ch.datascience.triplesgenerator.eventprocessing.triplescuration
package datasets

import ch.datascience.graph.Schemas._
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.TopmostDataFinder.TopmostData
import io.circe.Json
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._
import monocle.function.Plated

private class TriplesUpdater {

  def mergeTopmostDataIntoTriples(curatedTriples: CuratedTriples, topmostData: TopmostData): CuratedTriples =
    curatedTriples.copy(
      triples = JsonLDTriples(Plated.transform(updateDataset(topmostData))(curatedTriples.triples.value))
    )

  private def updateDataset(topmostData: TopmostData): Json => Json = { json =>
    root.`@type`.each.string.getAll(json) match {
      case types if types.contains("http://schema.org/Dataset") =>
        json.get[EntityId]("@id") match {
          case Some(topmostData.datasetId) =>
            val noSameAsJson = json.remove((renku / "topmostSameAs").toString)
            noSameAsJson deepMerge Json.obj(
              (renku / "topmostSameAs").toString      -> topmostData.sameAs.asJsonLD.toJson,
              (renku / "topmostDerivedFrom").toString -> topmostData.derivedFrom.asJsonLD.toJson
            )
          case _ => json
        }
      case _ => json
    }
  }
}

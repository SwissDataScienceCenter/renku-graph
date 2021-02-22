package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.effect.IO
import ch.datascience.graph.model.events.CommitId
import ch.datascience.rdfstore.JsonLDTriples

trait CommitIdExtractor[Interpretation[_]] {
  def extractCommitId(triples: JsonLDTriples): Interpretation[CommitId]
}

object IOCommitIdExtractor {
  def apply(): CommitIdExtractor[IO] = ???
}

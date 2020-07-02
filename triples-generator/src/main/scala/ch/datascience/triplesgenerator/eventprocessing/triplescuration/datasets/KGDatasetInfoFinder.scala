package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import ch.datascience.graph.model.datasets.{IdSameAs, SameAs}

import scala.language.higherKinds

private trait KGDatasetInfoFinder[Interpretation[_]] {
  def findTopmostSameAs(idSameAs: IdSameAs): Interpretation[Option[SameAs]]
}

private class KGDatasetInfoFinderImpl[Interpretation[_]]() extends KGDatasetInfoFinder[Interpretation] {
  override def findTopmostSameAs(idSameAs: IdSameAs): Interpretation[Option[SameAs]] = ???
}

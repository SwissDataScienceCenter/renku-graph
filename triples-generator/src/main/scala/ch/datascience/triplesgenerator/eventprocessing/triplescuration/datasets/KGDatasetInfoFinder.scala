package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import ch.datascience.graph.model.datasets.{DerivedFrom, IdSameAs, SameAs}

import scala.language.higherKinds

private trait KGDatasetInfoFinder[Interpretation[_]] {
  def findTopmostSameAs(idSameAs:         IdSameAs):    Interpretation[Option[SameAs]]
  def findTopmostDerivedFrom(derivedFrom: DerivedFrom): Interpretation[Option[DerivedFrom]]
}

private class KGDatasetInfoFinderImpl[Interpretation[_]]() extends KGDatasetInfoFinder[Interpretation] {
  override def findTopmostSameAs(idSameAs:         IdSameAs):    Interpretation[Option[SameAs]]      = ???
  override def findTopmostDerivedFrom(derivedFrom: DerivedFrom): Interpretation[Option[DerivedFrom]] = ???
}

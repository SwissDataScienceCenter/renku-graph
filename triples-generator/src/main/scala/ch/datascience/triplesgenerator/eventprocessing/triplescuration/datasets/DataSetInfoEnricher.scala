package ch.datascience.triplesgenerator.eventprocessing.triplescuration
package datasets

import cats.MonadError
import cats.data.EitherT
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs}
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError

import scala.language.higherKinds

private[triplescuration] class DataSetInfoEnricher[Interpretation[_]](
    dataSetInfoFinder: DataSetInfoFinder,
    updatesCreator:    UpdatesCreator
)(implicit ME:         MonadError[Interpretation, Throwable]) {

  def enrichDataSetInfo(curatedTriples: CuratedTriples): CurationResults[Interpretation] =
    EitherT.rightT[Interpretation, ProcessingRecoverableError] {
      dataSetInfoFinder
        .findDatasetsInfo(curatedTriples.triples)
        .foldLeft(curatedTriples) {
          case (curated, entityId) =>
            curated.addUpdates(updatesCreator.prepareUpdates(entityId, SameAs(entityId), DerivedFrom(entityId)))
        }
    }
}

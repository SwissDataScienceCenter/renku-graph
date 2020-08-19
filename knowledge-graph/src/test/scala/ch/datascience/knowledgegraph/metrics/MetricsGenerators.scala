package ch.datascience.knowledgegraph.metrics

import ch.datascience.knowledgegraph.metrics.KGEntityType.{Dataset, ProcessRun, Project}
import org.scalacheck.Gen

object MetricsGenerators {

  implicit val entitiesType: Gen[KGEntityType] = Gen.oneOf(
    Dataset,
    Project,
    ProcessRun
  )
}

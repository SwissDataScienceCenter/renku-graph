package io.renku.eventlog.metrics

import cats.effect.IO
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{Gauge, LabeledGauge, MetricsRegistry}
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger

object UnderTransformationGauge {

  def apply(
      metricsRegistry: MetricsRegistry[IO],
      statsFinder:     StatsFinder[IO],
      logger:          Logger[IO]
  ): IO[LabeledGauge[IO, projects.Path]] =
    Gauge[IO, projects.Path](
      name = "events_under_transformation_count",
      help = "Number of Events under triples transformation by project path.",
      labelName = "project",
      resetDataFetch =
        () => statsFinder.countEvents(Set(TransformingTriples: EventStatus)).map(_.view.mapValues(_.toDouble).toMap)
    )(metricsRegistry)
}

package io.renku.gitlab

import cats.effect.Sync
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.auto._
import io.renku.logging.ExecutionTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.prometheus.client.Histogram
import org.typelevel.log4cats.Logger

class ApiCallRecorder[F[_]](val instance: ExecutionTimeRecorder[F])

object ApiCallRecorder {

  private val ApiCallCountLabel: String Refined NonEmpty = "call_id"
  private lazy val apiCallCountsHistogram = Histogram
    .build()
    .name("gitlab_api_calls")
    .labelNames(ApiCallCountLabel.value)
    .help("GitLab API Calls")
    .buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 25, 50)

  def apply[F[_]: Sync: Logger](metricsRegistry: MetricsRegistry): F[ApiCallRecorder[F]] = for {
    histogram             <- metricsRegistry.register[F, Histogram, Histogram.Builder](apiCallCountsHistogram)
    executionTimeRecorder <- ExecutionTimeRecorder[F](maybeHistogram = Some(histogram))
  } yield new ApiCallRecorder(executionTimeRecorder)
}

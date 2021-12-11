package io.renku.metrics

import cats.effect.Sync
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.Histogram
import io.renku.logging.ExecutionTimeRecorder
import org.typelevel.log4cats.Logger

class GitLabApiCallRecorder[F[_]](val instance: ExecutionTimeRecorder[F])

object GitLabApiCallRecorder {

  private val ApiCallCountLabel: String Refined NonEmpty = "call_id"
  private lazy val apiCallCountsHistogram = Histogram
    .build()
    .name("gitlab_api_calls")
    .labelNames(ApiCallCountLabel.value)
    .help("GitLab API Calls")
    .buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10)

  def apply[F[_]: Sync: Logger](metricsRegistry: MetricsRegistry): F[GitLabApiCallRecorder[F]] = for {
    histogram             <- metricsRegistry.register[F, Histogram, Histogram.Builder](apiCallCountsHistogram)
    executionTimeRecorder <- ExecutionTimeRecorder[F](maybeHistogram = Some(histogram))
  } yield new GitLabApiCallRecorder(executionTimeRecorder)
}

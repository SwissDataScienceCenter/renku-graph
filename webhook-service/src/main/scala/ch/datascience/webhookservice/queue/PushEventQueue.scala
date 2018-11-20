package ch.datascience.webhookservice.queue

import akka.event.LoggingAdapter
import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Materializer, QueueOfferResult}
import akka.{Done, NotUsed}
import ch.datascience.webhookservice.PushEvent
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

class PushEventQueue(triplesFinder: TriplesFinder,
                     jenaConnector: FusekiConnector,
                     queueConfig: QueueConfig,
                     logger: LoggingAdapter)
                    (implicit executionContext: ExecutionContext, materializer: Materializer) {

  import queueConfig._

  def offer(pushEvent: PushEvent): Future[QueueOfferResult] =
    queue.offer(pushEvent)

  private lazy val queue = Source.queue[PushEvent](
    bufferSize.value,
    overflowStrategy = backpressure
  ).mapAsync(triplesFinderThreads.value)(pushEventToTriples)
    .flatMapConcat(logAndSkipErrors)
    .toMat(fusekiSink)(Keep.left)
    .run()

  private def pushEventToTriples(pushEvent: PushEvent): Future[(PushEvent, Either[Throwable, TriplesFile])] =
    triplesFinder.generateTriples(pushEvent.gitRepositoryUrl, pushEvent.checkoutSha)
      .map(maybeTriplesFile => pushEvent -> maybeTriplesFile)

  private lazy val logAndSkipErrors: ((PushEvent, Either[Throwable, TriplesFile])) => Source[(PushEvent, TriplesFile), NotUsed] = {
    case (event, Left(exception))    =>
      logger.error(s"Generating triples for $event failed: ${exception.getMessage}")
      Source.empty[(PushEvent, TriplesFile)]
    case (event, Right(triplesFile)) =>
      Source.single(event -> triplesFile)
  }

  private val fusekiSink: Sink[(PushEvent, TriplesFile), Future[Done]] =
    Flow[(PushEvent, TriplesFile)]
      .mapAsyncUnordered(1) {
        case (event, triplesFile) =>
          jenaConnector
            .uploadFile(triplesFile)
            .recover {
              case exception =>
                logger.error(s"Uploading triples for $event failed: ${exception.getMessage}")
                Done
            }
      }
      .toMat(Sink.ignore)(Keep.right)
}

object PushEventQueue {

  def apply(config: Config, logger: LoggingAdapter)
           (implicit executionContext: ExecutionContext, materializer: Materializer): PushEventQueue =
    new PushEventQueue(TriplesFinder(), FusekiConnector(config), QueueConfig(config), logger)
}

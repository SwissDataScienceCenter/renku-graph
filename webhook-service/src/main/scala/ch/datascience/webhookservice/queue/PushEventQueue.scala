package ch.datascience.webhookservice.queue

import akka.event.LoggingAdapter
import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Materializer, QueueOfferResult}
import akka.{Done, NotUsed}
import ch.datascience.webhookservice.PushEvent
import com.typesafe.config.Config
import org.apache.jena.graph.Graph

import scala.concurrent.{ExecutionContext, Future}

class PushEventQueue(tripletsFinder: TripletsFinder,
                     jenaConnector: JenaConnector,
                     queueConfig: QueueConfig,
                     logger: LoggingAdapter)
                    (implicit executionContext: ExecutionContext, materializer: Materializer) {

  import queueConfig._

  def offer(pushEvent: PushEvent): Future[QueueOfferResult] =
    queue.offer(pushEvent)

  private lazy val queue = Source.queue[PushEvent](
    bufferSize.value,
    overflowStrategy = backpressure
  ).mapAsync(tripletsFinderThreads.value)(pushEventToTriplets)
    .flatMapConcat(logAndSkipErrors)
    .toMat(jenaSink)(Keep.left)
    .run()

  private def pushEventToTriplets(pushEvent: PushEvent): Future[(PushEvent, Either[Throwable, Graph])] =
    tripletsFinder.findRdfGraph(pushEvent.gitRepositoryUrl, pushEvent.checkoutSha)
      .map(maybeTriplets => pushEvent -> maybeTriplets)

  private lazy val logAndSkipErrors: ((PushEvent, Either[Throwable, Graph])) => Source[(PushEvent, Graph), NotUsed] = {
    case (event, Left(exception)) =>
      logger.error(s"Finding triplets for $event failed: ${exception.getMessage}")
      Source.empty[(PushEvent, Graph)]
    case (event, Right(graph))    =>
      Source.single(event -> graph)
  }

  private val jenaSink: Sink[(PushEvent, Graph), Future[Done]] =
    Flow[(PushEvent, Graph)]
      .mapAsyncUnordered(1) {
        case (event, graph) =>
          jenaConnector
            .persist(graph)
            .recover {
              case exception =>
                logger.error(s"Persisting triplets for $event failed: ${exception.getMessage}")
                Done
            }
      }
      .toMat(Sink.ignore)(Keep.right)
}

object PushEventQueue {

  def apply(config: Config, logger: LoggingAdapter)
           (implicit executionContext: ExecutionContext, materializer: Materializer): PushEventQueue =
    new PushEventQueue(TripletsFinder(), new JenaConnector, QueueConfig(config), logger)
}

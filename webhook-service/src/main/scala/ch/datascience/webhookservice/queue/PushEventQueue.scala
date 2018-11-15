package ch.datascience.webhookservice.queue

import akka.event.LoggingAdapter
import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, QueueOfferResult}
import akka.{Done, NotUsed}
import ch.datascience.webhookservice.PushEvent
import com.typesafe.config.Config
import org.apache.jena.graph.Graph
import org.w3.banana.jena.Jena

import scala.concurrent.{ExecutionContext, Future}

class PushEventQueue(tripletsFinder: TripletsFinder, queueConfig: QueueConfig, logger: LoggingAdapter)
                    (implicit executionContext: ExecutionContext, materializer: Materializer) {

  import queueConfig._

  def offer(pushEvent: PushEvent): Future[QueueOfferResult] =
    queue.offer(pushEvent)

  private lazy val queue = Source.queue[PushEvent](
    bufferSize.value,
    overflowStrategy = backpressure
  ).mapAsync(tripletsFinderThreads.value)(pushEventToTriplets)
    .flatMapConcat(logAndSkipErrors)
    .toMat(sink)(Keep.left)
    .run()

  private def pushEventToTriplets(pushEvent: PushEvent): Future[(PushEvent, Either[Throwable, Jena#Graph])] =
    tripletsFinder.findRdfGraph(pushEvent.gitRepositoryUrl, pushEvent.checkoutSha)
      .map(maybeTriplets => pushEvent -> maybeTriplets)

  private lazy val logAndSkipErrors: ((PushEvent, Either[Throwable, Jena#Graph])) => Source[Jena#Graph, NotUsed] = {
    case (event, Left(exception)) =>
      logger.error(s"$event processing failed: ${exception.getMessage}")
      Source.empty[Jena#Graph]
    case (_, Right(graph))        =>
      Source.single(graph)
  }

  private val sink: Sink[Graph, Future[Done]] = Sink.foreach[Graph](g =>
    println(g)
  )
}

object PushEventQueue {

  def apply(config: Config, logger: LoggingAdapter)
           (implicit executionContext: ExecutionContext, materializer: Materializer): PushEventQueue =
    new PushEventQueue(TripletsFinder(), QueueConfig(config), logger)
}

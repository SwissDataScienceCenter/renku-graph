package ch.datascience.webhookservice.queue

import akka.Done
import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, QueueOfferResult}
import ch.datascience.webhookservice.PushEvent
import org.w3.banana.jena.Jena

import scala.concurrent.{ExecutionContext, Future}

class PushEventQueue(tripletsFinder: TripletsFinder, queueConfig: QueueConfig)
                    (implicit executionContext: ExecutionContext, materializer: Materializer) {

  import queueConfig._

  private val parallelism = 20

  private val sink: Sink[Either[Throwable, Jena#Graph], Future[Done]] = Sink.foreach(println)

  private lazy val queue = Source.queue[PushEvent](
    bufferSize.value,
    overflowStrategy = backpressure
  ).mapAsync(tripletsFinderThreads.value) { event =>
    tripletsFinder.findRdfGraph(event.gitRepositoryUrl, event.checkoutSha)
  }.toMat(sink)(Keep.left)
    .run()

  def offer(pushEvent: PushEvent): Future[QueueOfferResult] =
    queue.offer(pushEvent)
}

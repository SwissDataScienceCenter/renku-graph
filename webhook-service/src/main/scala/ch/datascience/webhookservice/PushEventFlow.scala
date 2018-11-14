package ch.datascience.webhookservice

import akka.Done
import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, QueueOfferResult}
import ch.datascience.webhookservice.config.BufferSize
import ch.datascience.webhookservice.triplets.TripletsFinder

import scala.concurrent.{ExecutionContext, Future}

class PushEventFlow(tripletsFinder: TripletsFinder, bufferSize: BufferSize)
                   (implicit executionContext: ExecutionContext, materializer: Materializer) {

  private val parallelism = 20

  private val sink: Sink[Either[Throwable, String], Future[Done]] = Sink.foreach(println)

  private lazy val queue = Source.queue[PushEvent](
    bufferSize.value,
    overflowStrategy = backpressure
  ).mapAsync(parallelism) { event =>
    tripletsFinder.findTriplets(event.gitRepositoryUrl, event.checkoutSha)
  }.toMat(sink)(Keep.left)
    .run()

  def offer(pushEvent: PushEvent): Future[QueueOfferResult] =
    queue.offer(pushEvent)
}

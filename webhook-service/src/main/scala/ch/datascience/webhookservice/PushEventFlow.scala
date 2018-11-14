package ch.datascience.webhookservice

import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{Materializer, QueueOfferResult}
import ch.datascience.webhookservice.config.BufferSize
import ch.datascience.webhookservice.triplets.TripletsFinder

import scala.concurrent.{ExecutionContext, Future}

class PushEventFlow(tripletsFinder: TripletsFinder, bufferSize: BufferSize)
                   (implicit executionContext: ExecutionContext, materializer: Materializer) {

  private lazy val queue = Source.queue[PushEvent](
    bufferSize.value,
    overflowStrategy = backpressure
  ).mapAsync(2) { event =>
    tripletsFinder.findTriplets(event.gitRepositoryUrl, event.checkoutSha)
  }.toMat(Sink.foreach(println))(Keep.left)
    .run()

  def offer(pushEvent: PushEvent): Future[QueueOfferResult] =
    queue.offer(pushEvent)
}

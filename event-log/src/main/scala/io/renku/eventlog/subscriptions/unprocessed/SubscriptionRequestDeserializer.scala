package io.renku.eventlog.subscriptions.unprocessed

import cats.MonadError
import cats.implicits.catsSyntaxApplicativeId
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.tinytypes.json.TinyTypeDecoders.stringDecoder
import io.circe.{Decoder, Json}
import io.renku.eventlog.subscriptions.unprocessed.UnprocessedSubscriptionRequestDeserializer.UrlAndStatuses
import io.renku.eventlog.subscriptions.{SubscriberUrl, SubscriptionRequest, SubscriptionRequestDeserializer}

// TODO: make this private[unprocessed]
private[eventlog] case class UnprocessedSubscriptionRequestDeserializer[Interpretation[_]]()(implicit
    monadError: MonadError[Interpretation, Throwable]
) extends SubscriptionRequestDeserializer[Interpretation, UrlAndStatuses] {
  override def deserialize(payload: Json): Interpretation[UrlAndStatuses] =
    payload
      .as[UrlAndStatuses]
      .fold(monadError.raiseError[UrlAndStatuses](_), urlAndStatuses => urlAndStatuses.pure[Interpretation])

  implicit val payloadDecoder: Decoder[UrlAndStatuses] = cursor =>
    for {
      subscriberUrl <- cursor.downField("subscriberUrl").as[SubscriberUrl](stringDecoder(SubscriberUrl))
      statuses      <- cursor.downField("statuses").as[List[EventStatus]]
    } yield UrlAndStatuses(subscriberUrl, statuses.toSet)
}

object UnprocessedSubscriptionRequestDeserializer {

  case class UrlAndStatuses(subscriberUrl: SubscriberUrl, eventStatuses: Set[EventStatus]) extends SubscriptionRequest

}

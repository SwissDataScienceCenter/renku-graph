package io.renku.eventlog.subscriptions

import io.circe.{DecodingFailure, Json}

trait SubscriptionRequestDeserializer[Interpretation[_], T <: SubscriptionRequest] {

  def deserialize(payload: Json): Interpretation[T]
}

trait SubscriptionRequest

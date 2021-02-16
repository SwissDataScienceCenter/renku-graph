package io.renku.eventlog.events.categories.creation

import cats.effect.IO
import ch.datascience.events.consumers.EventConsumersRegistry
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.interpreters.TestLogger
import io.renku.eventlog.events.EventEndpointImpl
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventHandlerSpec extends AnyWordSpec with should.Matchers {

  "handleEvent" should {

    //    "decode an Event from the request, " +
    //      "create it in the Event Log " +
    //      s"and return $Created " +
    //      "if there's no such an event in the Log yet" in new TestCase {
    //
    //        val event = newEvents.generateOne
    //        (persister.storeNewEvent _)
    //          .expects(event)
    //          .returning(Result.Created.pure[IO])
    //
    //        val request = Request(Method.POST, uri"events").withEntity(event.asJson)
    //
    //        val response = addEvent(request).unsafeRunSync()
    //
    //        response.status                          shouldBe Created
    //        response.contentType                     shouldBe Some(`Content-Type`(application.json))
    //        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event created")
    //
    //        logger.loggedOnly(Info(s"Event ${event.compoundEventId}, projectPath = ${event.project.path} added"))
    //      }
    //
    //    "decode an Event from the request, " +
    //      "create it in the Event Log " +
    //      s"and return $Ok " +
    //      "if such an event was already in the Log" in new TestCase {
    //
    //        val event = newEvents.generateOne
    //        (persister.storeNewEvent _)
    //          .expects(event)
    //          .returning(Result.Existed.pure[IO])
    //
    //        val request = Request(Method.POST, uri"events").withEntity(event.asJson)
    //
    //        val response = addEvent(request).unsafeRunSync()
    //
    //        response.status                          shouldBe Ok
    //        response.contentType                     shouldBe Some(`Content-Type`(application.json))
    //        response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event existed")
    //
    //        logger.expectNoLogs()
    //      }
    //
    //    s"return $Ok if status was SKIPPED *and* the message is not blank" in new TestCase {
    //      val event = skippedEvents.generateOne
    //      (persister.storeNewEvent _)
    //        .expects(event)
    //        .returning(Result.Existed.pure[IO])
    //      val request  = Request(Method.POST, uri"events").withEntity(event.asJson)
    //      val response = addEvent(request).unsafeRunSync()
    //      response.status                          shouldBe Ok
    //      response.contentType                     shouldBe Some(`Content-Type`(application.json))
    //      response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event existed")
    //
    //      logger.expectNoLogs()
    //    }
    //
    //    s"return $BadRequest if status was SKIPPED *but* the message is blank" in new TestCase {
    //
    //      val invalidPayload: Json = skippedEvents.generateOne.asJson.hcursor
    //        .downField("status")
    //        .delete
    //        .as[Json]
    //        .fold(throw _, identity)
    //        .deepMerge(json"""{"status": ${blankStrings().generateOne}}""")
    //      val request  = Request(Method.POST, uri"events").withEntity(invalidPayload)
    //      val response = addEvent(request).unsafeRunSync()
    //      response.status      shouldBe BadRequest
    //      response.contentType shouldBe Some(`Content-Type`(application.json))
    //      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(
    //        s"Invalid message body: Could not decode JSON: ${invalidPayload.toString}"
    //      )
    //
    //      logger.expectNoLogs()
    //    }
    //
    //    s"set default status to NEW if no status is provided" in new TestCase {
    //      val newEvent = newEvents.generateOne
    //      (persister.storeNewEvent _)
    //        .expects(newEvent)
    //        .returning(Result.Existed.pure[IO])
    //      val payloadWithoutStatus = newEvent.asJson.hcursor.downField("status").delete.as[Json].fold(throw _, identity)
    //      val request              = Request(Method.POST, uri"events").withEntity(payloadWithoutStatus)
    //      val response             = addEvent(request).unsafeRunSync()
    //      response.status                          shouldBe Ok
    //      response.contentType                     shouldBe Some(`Content-Type`(application.json))
    //      response.as[InfoMessage].unsafeRunSync() shouldBe InfoMessage("Event existed")
    //
    //      logger.expectNoLogs()
    //    }
    //
    //    unacceptableStatuses.foreach { invalidStatus =>
    //      s"return $BadRequest if status was $invalidStatus" in new TestCase {
    //
    //        val randomEvent          = newOrSkippedEvents.generateOne
    //        val payloadInvalidStatus = randomEvent.asJson.deepMerge(json"""{"status": ${invalidStatus.value} }""")
    //
    //        val request  = Request(Method.POST, uri"events").withEntity(payloadInvalidStatus)
    //        val response = addEvent(request).unsafeRunSync()
    //        response.status      shouldBe BadRequest
    //        response.contentType shouldBe Some(`Content-Type`(application.json))
    //        response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(
    //          s"Invalid message body: Could not decode JSON: $payloadInvalidStatus"
    //        )
    //        logger.expectNoLogs()
    //      }
    //
    //    }
    //
    //    s"return $BadRequest if decoding Event from the request fails" in new TestCase {
    //
    //      val payload = jsons.generateOne
    //      val request = Request(Method.POST, uri"events").withEntity(payload)
    //
    //      val response = addEvent(request).unsafeRunSync()
    //
    //      response.status      shouldBe BadRequest
    //      response.contentType shouldBe Some(`Content-Type`(application.json))
    //      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage(
    //        s"Invalid message body: Could not decode JSON: $payload"
    //      )
    //
    //      logger.expectNoLogs()
    //    }
    //
    //    s"return $InternalServerError when storing Event in the Log fails" in new TestCase {
    //
    //      val event     = newEvents.generateOne
    //      val exception = exceptions.generateOne
    //      (persister.storeNewEvent _)
    //        .expects(event)
    //        .returning(exception.raiseError[IO, EventPersister.Result])
    //
    //      val request = Request(Method.POST, uri"events").withEntity(event.asJson)
    //
    //      val response = addEvent(request).unsafeRunSync()
    //
    //      response.status                   shouldBe InternalServerError
    //      response.contentType              shouldBe Some(`Content-Type`(MediaType.application.json))
    //      response.as[Json].unsafeRunSync() shouldBe ErrorMessage("Event creation failed").asJson
    //
    //      logger.loggedOnly(Error("Event creation failed", exception))
    //    }
  }

  private trait TestCase {

    val logger = TestLogger[IO]()
//    val processEvent = new EventEndpointImpl[IO](eventConsumersRegistry, logger).processEvent _
  }

  private lazy val unacceptableStatuses = EventStatus.all.diff(Set(EventStatus.New, EventStatus.Skipped))

  //private implicit def eventEncoder[T <: Event]: Encoder[T] = Encoder.instance[T] {
//  case event: NewEvent     => toJson(event)
//  case event: SkippedEvent => toJson(event) deepMerge json"""{ "message":    ${event.message.value} }"""
//}
//
//  private def toJson(event: Event): Json = json"""{
//    "id":         ${event.id.value},
//    "project":    ${event.project},
//    "date":       ${event.date.value},
//    "batchDate":  ${event.batchDate.value},
//    "body":       ${event.body.value},
//    "status":     ${event.status.value}
//  }"""
//
//  private implicit lazy val projectEncoder: Encoder[EventProject] = Encoder.instance[EventProject] { project =>
//  json"""{
//      "id":   ${project.id.value},
//      "path": ${project.path.value}
//    }"""
//}

}

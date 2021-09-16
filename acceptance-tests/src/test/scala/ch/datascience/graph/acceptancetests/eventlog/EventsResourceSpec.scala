package ch.datascience.graph.acceptancetests.eventlog

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data.Project.Statistics.CommitsCount
import ch.datascience.graph.acceptancetests.data.{RdfStoreData, dataProjects}
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.GraphServices
import ch.datascience.graph.acceptancetests.tooling.ResponseTools._
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.events.{EventId, EventStatus}
import ch.datascience.graph.model.testentities.generators.EntitiesGenerators
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.UrlEncoder.urlEncode
import eu.timepit.refined.auto._
import io.circe.{Decoder, Json}
import io.renku.jsonld.syntax._
import org.http4s.Status._
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should

class EventsResourceSpec
    extends AnyFeatureSpec
    with GivenWhenThen
    with GraphServices
    with AcceptanceTestPatience
    with RdfStoreData
    with should.Matchers
    with Eventually
    with EntitiesGenerators {

  Feature("GET /events?project-path=<path> to return info about all the project events") {

    Scenario("As a user I would like to see all events from the project with the given path") {
      val commits = commitIds.generateNonEmptyList(maxElements = 6).toList
      implicit val accessToken: AccessToken = accessTokens.generateOne
      val project = dataProjects(projectEntities(anyVisibility), CommitsCount(commits.size)).generateOne

      When("there are no events for the given project in EL")
      Then("the resource should return OK with an empty array")
      val noEventsResponse = eventLogClient.GET(s"events?project-path=${urlEncode(project.path.show)}")
      noEventsResponse.status                    shouldBe Ok
      noEventsResponse.bodyAsJson.as[List[Json]] shouldBe Nil.asRight

      commits foreach `data in the RDF store`(project, project.entitiesProject.asJsonLD, _)

      eventually {
        val eventsResponse = eventLogClient.GET(s"events?project-path=${urlEncode(project.path.show)}")
        eventsResponse.status shouldBe Ok
        val Right(events) = noEventsResponse.bodyAsJson.as[List[EventInfo]]
        events.size               shouldBe commits.size
        events.map(_.eventId.value) should contain theSameElementsAs commits.map(_.value)
      }
    }
  }

  private final case class EventInfo(eventId: EventId, status: EventStatus, maybeMessage: Option[String])

  private implicit lazy val infoDecoder: Decoder[EventInfo] = cursor => {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    for {
      id           <- cursor.downField("id").as[EventId]
      status       <- cursor.downField("status").as[EventStatus]
      maybeMessage <- cursor.downField("message").as[Option[String]]
    } yield EventInfo(id, status, maybeMessage)
  }
}

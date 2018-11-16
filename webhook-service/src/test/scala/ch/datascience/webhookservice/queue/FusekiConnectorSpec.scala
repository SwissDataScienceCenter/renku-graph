package ch.datascience.webhookservice.queue

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.webhookservice.ProjectName
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.queue.FusekiConnector.FusekiUrl
import org.apache.jena.rdfconnection.RDFConnection
import org.scalamock.function.MockFunction1
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.{global => executionContext}
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

class FusekiConnectorSpec extends WordSpec with MockFactory with ScalaFutures {

  "uploadFile" should {

    "upload the content of the given file to Jena Fuseki" in new TestCase {

      fusekiConnectionBuilder
        .expects(FusekiUrl(s"$fusekiBaseUrl/$projectName"))
        .returning(fusekiConnection)

      (fusekiConnection.load(_: String))
        .expects(triplesFile.value)

      (fusekiConnection.close _)
        .expects()

      fusekiConnector.uploadFile(triplesFile, projectName).futureValue
    }

    "return failure if upload to Jena Fuseki fails" in new TestCase {

      fusekiConnectionBuilder
        .expects(FusekiUrl(s"$fusekiBaseUrl/$projectName"))
        .returning(fusekiConnection)

      val exception: Exception = new Exception("message")
      (fusekiConnection.load(_: String))
        .expects(triplesFile.value)
        .throwing(exception)

      (fusekiConnection.close _)
        .expects()

      intercept[Exception] {
        Await.result(fusekiConnector.uploadFile(triplesFile, projectName), 1 second)
      } shouldBe exception
    }

    "return failure if creating an url to fuseki fails" in new TestCase {

      val exception: Exception = new Exception("message")
      fusekiConnectionBuilder
        .expects(FusekiUrl(s"$fusekiBaseUrl/$projectName"))
        .throwing(exception)

      intercept[Exception] {
        Await.result(fusekiConnector.uploadFile(triplesFile, projectName), 1 second)
      } shouldBe exception
    }

    "return failure if closing connection to fuseki fails" in new TestCase {

      fusekiConnectionBuilder
        .expects(FusekiUrl(s"$fusekiBaseUrl/$projectName"))
        .returning(fusekiConnection)

      (fusekiConnection.load(_: String))
        .expects(triplesFile.value)

      val exception: Exception = new Exception("message")
      (fusekiConnection.close _)
        .expects().twice()
        .throwing(exception)

      intercept[Exception] {
        Await.result(fusekiConnector.uploadFile(triplesFile, projectName), 1 second)
      } shouldBe exception
    }
  }

  private trait TestCase {
    val projectName: ProjectName = projectNames.generateOne
    val triplesFile: TriplesFile = triplesFiles.generateOne

    val fusekiConnectionBuilder: MockFunction1[FusekiUrl, RDFConnection] = mockFunction[FusekiUrl, RDFConnection]
    val fusekiConnection: RDFConnection = mock[RDFConnection]
    val fusekiBaseUrl = FusekiUrl("http://localhost:3030")
    val fusekiConnector = new FusekiConnector(fusekiBaseUrl, fusekiConnectionBuilder)
  }
}
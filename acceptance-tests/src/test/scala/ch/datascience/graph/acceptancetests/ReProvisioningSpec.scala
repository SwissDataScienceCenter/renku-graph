package ch.datascience.graph.acceptancetests

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.data._
import ch.datascience.graph.acceptancetests.flows.RdfStoreProvisioning.`data in the RDF store`
import ch.datascience.graph.acceptancetests.stubs.GitLab.`GET <gitlabApi>/projects/:path returning OK with`
import ch.datascience.graph.acceptancetests.stubs.RdfStoreStub
import ch.datascience.graph.acceptancetests.tooling.GraphServices._
import ch.datascience.graph.acceptancetests.tooling.ResponseTools.ResponseOps
import ch.datascience.graph.acceptancetests.tooling.{GraphServices, ModelImplicits, RDFStore, ServiceRun}
import ch.datascience.graph.model
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.projects.Visibility
import ch.datascience.http.client.AccessToken
import ch.datascience.knowledgegraph.projects.ProjectsGenerators.projects
import ch.datascience.rdfstore.entities.EntitiesGenerators.persons
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.triplesgenerator
import io.circe.Json
import io.renku.jsonld._
import org.http4s.Status.Ok
import org.scalactic.source.Position
import org.scalatest.GivenWhenThen
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.enablers.Retrying
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should
import org.scalatest.time.{Millis, Minutes, Span}

class ReProvisioningSpec
    extends AnyFeatureSpec
    with ModelImplicits
    with GivenWhenThen
    with GraphServices
    with should.Matchers {

  Feature("ReProvisioning") {

    implicit val accessToken: AccessToken = accessTokens.generateOne
    val initialProjectVersion = SchemaVersion("0.15.0")
    val project =
      projects.generateOne.copy(path = model.projects.Path("public/re-provisioning"),
                                visibility = Visibility.Public,
                                version = initialProjectVersion
      )
    val commitId  = commitIds.generateOne
    val committer = persons.generateOne

    Scenario("Update CLI version and expect re-provisioning") {
      Given("The TG is using an older version of the CLI")

      And("There is data from this version in Jena")
      val jsonTriples = nonModifiedDataSetCommit(commitId = commitId, committer = committer)(
        projectPath = project.path,
        projectName = project.name,
        projectDateCreated = project.created.date,
        maybeProjectCreator = project.created.maybeCreator.map(creator => Person(creator.name, creator.maybeEmail)),
        projectVersion = project.version
      )()

      val projectjsonLDTriples = JsonLD.arr(jsonTriples)

      `data in the RDF store`(project, commitId, committer, projectjsonLDTriples)()

      `GET <gitlabApi>/projects/:path returning OK with`(project, maybeCreator = committer.some, withStatistics = true)
      val projectDetailsResponse = knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

      projectDetailsResponse.status shouldBe Ok
      val Right(projectDetails) = projectDetailsResponse.bodyAsJson.as[Json]
      val Right(version)        = projectDetails.hcursor.downField("version").as[String]
      version shouldBe initialProjectVersion.value

      When("The compatibility matrix is updated and the TG is restarted")
      restartTGWithNewCompatMatrix("application-re-provisioning.conf")

      Then("Re-provisioning is triggered")

      And("The new data can be queried in Jena")

      `GET <gitlabApi>/projects/:path returning OK with`(project, maybeCreator = committer.some, withStatistics = true)

      val patience: org.scalatest.concurrent.Eventually.PatienceConfig =
        Eventually.PatienceConfig(timeout = Span(20, Minutes), interval = Span(30000, Millis))

      eventually {
        println("Retrying")
        val updatedProjectDetailsResponse =
          knowledgeGraphClient.GET(s"knowledge-graph/projects/${project.path}", accessToken)

        updatedProjectDetailsResponse.status shouldBe Ok
        val Right(updatedProjectDetails) = updatedProjectDetailsResponse.bodyAsJson.as[Json]
        val Right(updatedVersion)        = updatedProjectDetails.hcursor.downField("version").as[String]
        updatedVersion shouldBe "0.16.0"
      }(patience, Retrying.retryingNatureOfT, Position.here)

    }
  }

  private def restartTGWithNewCompatMatrix(configFilename: String): Unit = {
    val newTriplesGenerator = ServiceRun(
      "triples-generator",
      service = triplesgenerator.Microservice,
      serviceClient = triplesGeneratorClient,
      preServiceStart = List(RDFStore.stop(), IO(RdfStoreStub.start()), IO(RdfStoreStub.givenRenkuDatasetExists())),
      postServiceStart = List(IO(RdfStoreStub.shutdown()), RDFStore.start()),
      serviceArgsList = List(() => configFilename)
    )
    stop(newTriplesGenerator.name)
    GraphServices.run(newTriplesGenerator)
  }
}

package ch.datascience.graph.acceptancetests

import cats.effect.IO
import ch.datascience.graph.acceptancetests.stubs.RdfStoreStub
import ch.datascience.graph.acceptancetests.tooling.GraphServices._
import ch.datascience.graph.acceptancetests.tooling.{GraphServices, ModelImplicits, RDFStore, ServiceRun}
import ch.datascience.triplesgenerator
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should

class ReProvisioningSpec
    extends AnyFeatureSpec
    with ModelImplicits
    with GivenWhenThen
    with GraphServices
    with should.Matchers {

  Feature("ReProvisioning") {
    Scenario("Update CLI version and expect re-provisioning") {
      Given("The TG is using an older version of the CLI")

      And("There is data from this version in Jena")

      When("The compatibility matrix is updated and the TG is restarted")
      restartTGWithNewCompatMatrix("application-re-provisioning.conf")

      Then("Re-provisioning is triggered")

      And("The new data can be queried in Jena")
    }
  }

  private def restartTGWithNewCompatMatrix(configFilename: String) = {
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

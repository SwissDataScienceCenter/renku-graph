package ch.datascience.graphservice.rdfstore

import ch.datascience.generators.Generators.{httpUrls, nonEmptyStrings}
import org.scalacheck.Gen

object RDFStoreGenerators {

  import RDFStoreConfig._

  implicit val fusekiConfigs: Gen[RDFStoreConfig] = for {
    fusekiUrl   <- httpUrls map FusekiBaseUrl.apply
    datasetName <- nonEmptyStrings() map DatasetName.apply
  } yield RDFStoreConfig(fusekiUrl, datasetName)
}

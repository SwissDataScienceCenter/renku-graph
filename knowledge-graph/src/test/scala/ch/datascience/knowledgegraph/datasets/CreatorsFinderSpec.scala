package ch.datascience.knowledgegraph.datasets

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.blankStrings
import ch.datascience.graph.model.users.{Email, Name}
import ch.datascience.knowledgegraph.datasets.model.DatasetCreator
import io.circe.literal._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CreatorsFinderSpec extends WordSpec with ScalaCheckPropertyChecks {

  import CreatorsFinder._

  "dataset creator decoder" should {

    "decode result-set with a blank affiliation to a DatasetCreator object" in {
      forAll(emails, names, blankStrings()) { (email, name, affiliation) =>
        resultSet(email, name, affiliation).as[List[DatasetCreator]] shouldBe Right {
          List(DatasetCreator(Some(email), name, None))
        }
      }
    }

    "decode result-set with a non-blank affiliation to a DatasetCreator object" in {
      forAll(emails, names, affiliations) { (email, name, affiliation) =>
        resultSet(email, name, affiliation.toString).as[List[DatasetCreator]] shouldBe Right {
          List(DatasetCreator(Some(email), name, Some(affiliation)))
        }
      }
    }
  }

  private def resultSet(email: Email, name: Name, blank: String) = json"""
  {
    "results": {
      "bindings": [
        {
          "email": {"value": ${email.value}},
          "name": {"value": ${name.value}},
          "affiliation": {"value": $blank}
        }
      ]
    }
  }"""
}

package io.renku.triplesstore.client

import io.renku.jsonld.EntityId
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, VarName}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class SyntaxTest extends AnyFlatSpec with should.Matchers {

  import syntax._

  it should "interpolate a lucene query as string" in {
    val query = LuceneQuery.escape("this is a query?")
    fr"?id text:query (schema:name $query)" shouldBe Fragment(s"?id text:query (schema:name '${query.query}')")
  }

  it should "interpolate strings" in {
    val name = "John"
    fr"?id schema:name $name" shouldBe Fragment(s"?id schema:name '$name'")
  }

  it should "interpolate entityIds" in {
    val id: EntityId = EntityId.blank
    fr"Graph $id {}" shouldBe Fragment(s"Graph <${id.value}> {}")

    val id2 = EntityId.of("http://localhost/resource/id2")
    fr"Graph $id2 {}" shouldBe Fragment(s"Graph <${id2.value}> {}")
  }

  it should "interpolate chars" in {
    val c = ','
    fr"separator=$c" shouldBe Fragment(s"separator=','")
  }

  it should "interpolate variables" in {
    val id = VarName("?id")
    fr"$id a renku:Dataset" shouldBe Fragment(s"?id a renku:Dataset")
  }

  it should "support stripMargin" in {
    val v1   = VarName("name")
    val name = "John"
    val fragment =
      fr"""
          |Select $v1
          |Where {
          |  $v1 schema:name $name
          |}
          |""".stripMargin

    fragment shouldBe Fragment(
      s"""
         |Select ?name
         |Where {
         |  ?name schema:name 'John'
         |}
         |""".stripMargin
    )
  }

  it should "interpolate option" in {
    val name: Option[String] = Some("John")
    fr"?id schema:name $name" shouldBe Fragment(s"?id schema:name 'John'")

    val noName: Option[String] = None
    fr"?id schema:name $noName" shouldBe Fragment(s"?id schema:name ")
  }
}

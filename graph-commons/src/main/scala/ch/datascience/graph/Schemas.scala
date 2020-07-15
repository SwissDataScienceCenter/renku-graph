package ch.datascience.graph

import io.renku.jsonld.Schema

trait Schemas {
  val prov:      Schema = Schema.from("http://www.w3.org/ns/prov", separator = "#")
  val wfprov:    Schema = Schema.from("http://purl.org/wf4ever/wfprov", separator = "#")
  val wfdesc:    Schema = Schema.from("http://purl.org/wf4ever/wfdesc", separator = "#")
  val rdfs:      Schema = Schema.from("http://www.w3.org/2000/01/rdf-schema", separator = "#")
  val xmlSchema: Schema = Schema.from("http://www.w3.org/2001/XMLSchema", separator = "#")
  val schema:    Schema = Schema.from("http://schema.org")
  val renku:     Schema = Schema.from("https://swissdatasciencecenter.github.io/renku-ontology", separator = "#")
}

object Schemas extends Schemas

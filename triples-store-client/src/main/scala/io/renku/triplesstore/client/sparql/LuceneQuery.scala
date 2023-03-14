package io.renku.triplesstore.client.sparql

final class LuceneQuery(val query: String) extends AnyVal

object LuceneQuery {
  val queryAll: LuceneQuery = LuceneQuery("*")

  def apply(str: String): LuceneQuery = new LuceneQuery(str)

  def escape(str: String): LuceneQuery = LuceneQuery(LuceneQueryEncoder.queryAsString(str))

  implicit val sparqlEncoder: SparqlEncoder[LuceneQuery] =
    SparqlEncoder.instance(q => Fragment(s"'${q.query}'"))
}

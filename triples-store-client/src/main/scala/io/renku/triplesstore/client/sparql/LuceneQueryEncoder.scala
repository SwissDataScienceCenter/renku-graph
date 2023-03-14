package io.renku.triplesstore.client.sparql

import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil

private object LuceneQueryEncoder {

  def queryAsString(v: String): String = QueryParserUtil.escape(v).replace("\\", "\\\\")
}

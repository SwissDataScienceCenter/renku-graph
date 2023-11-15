/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.triplesstore.client.sparql

import org.apache.lucene.analysis.core.LetterTokenizer
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, CharTermAttributeImpl}
import org.apache.lucene.analysis.{Analyzer, TokenStream, Tokenizer}
import org.apache.lucene.util.AttributeFactory
import org.apache.lucene.util.AttributeFactory.StaticImplementationAttributeFactory

import java.io.StringReader

trait QueryTokenizer { self =>
  def split(input: String): List[String]

  final def append(next: QueryTokenizer): QueryTokenizer =
    (input: String) => self.split(input).flatMap(next.split)
}

object QueryTokenizer {

  def luceneStandard: QueryTokenizer =
    new LuceneTokenizer(new StandardTokenizer(_))

  def luceneLetters: QueryTokenizer =
    new LuceneTokenizer(new LetterTokenizer(_))

  def lucenePreservingWithLetters: QueryTokenizer =
    new LuceneAnalyzer(
      CustomAnalyzer
        .builder()
        .withTokenizer("whitespace")
        .addTokenFilter("wordDelimiterGraph", "preserveOriginal", "1", "stemEnglishPossessive", "0")
        .build()
    )

  def splitOn(c: Char): QueryTokenizer =
    (input: String) => input.split(c).toList

  def default: QueryTokenizer =
    lucenePreservingWithLetters

  private final class LuceneTokenizer(createDelegate: AttributeFactory => Tokenizer) extends QueryTokenizer {

    override def split(input: String): List[String] = {
      val tokenizer = createDelegate(
        new StaticImplementationAttributeFactory[CharTermAttributeImpl](
          AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY,
          classOf[CharTermAttributeImpl]
        ) {
          override def createInstance(): CharTermAttributeImpl = new CharTermAttributeImpl
        }
      )
      tokenizer.setReader(new StringReader(input))
      val attr = tokenizer.addAttribute(classOf[CharTermAttribute])

      tokenizer.reset()

      collectTokens(tokenizer, attr).reverse
    }
  }

  private final class LuceneAnalyzer(analyzer: Analyzer) extends QueryTokenizer {

    override def split(input: String): List[String] = {

      val tokenStream = analyzer.tokenStream("DUMMY_FIELD", input)
      val attr        = tokenStream.addAttribute(classOf[CharTermAttribute])

      tokenStream.reset()

      collectTokens(tokenStream, attr).reverse
    }
  }

  @annotation.tailrec
  private def collectTokens(tokenStream: TokenStream,
                            attr:        CharTermAttribute,
                            results:     List[String] = Nil
  ): List[String] =
    if (tokenStream.incrementToken()) collectTokens(tokenStream, attr, attr.toString :: results)
    else results
}

# acceptance-tests

This module contains acceptance tests of the Graph Services.

In order to run them issue: `sbt "project acceptance-tests" clean test` from the project root.

In case there's a need of runnning a single test issue: `sbt "project acceptance-tests" "testOnly *<test-class-name>*"`

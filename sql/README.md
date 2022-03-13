# SQL module of Stalactite

This module is a small layer over JDBC to ease some manipulations over it such as :
* [Iterating](src/main/java/org/codefilarete/stalactite/sql/result/ResultSetIterator.java) over ResultSet and [build graph object from it](src/test/java/org/codefilarete/sql/result/ResultSetRowConverterTest.java)
* Dinstinguish [read](src/main/java/org/codefilarete/stalactite/sql/statement/ReadOperation.java) and [write](src/main/java/org/codefilarete/stalactite/sql/statement/WriteOperation.java) operation (thoses operations are mixed in Statement)
* [listen to transactions](src/main/java/org/codefilarete/stalactite/sql/TransactionListener.java)
* Set values through [binders](Stalactite/stalactite/sql/src/main/java/org/codefilarete/stalactite/sql/statement/binder/binders.md)
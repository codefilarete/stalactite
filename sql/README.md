# SQL module of Stalactite

This module is a small layer over JDBC to ease some manipulations over it such as :
* [Iterating](/src/main/java/org/gama/sql/result/ResultSetIterator.java) over ResultSet and [build graph object from it](src/test/java/org/gama/sql/result/ResultSetRowConverterTest.java)
* Dinstinguish [read](/src/main/java/org/gama/sql/dml/ReadOperation.java) and [write](src/main/java/org/gama/sql/dml/WriteOperation.java) operation (mixed in Statement)
* [Manage transactions](src/main/java/org/gama/sql/TransactionAdapter.java) and [listen to them](src/main/java/org/gama/sql/TransactionListener.java)
* Set values through [binders](src/main/java/org/gama/sql/binder/binders.md)
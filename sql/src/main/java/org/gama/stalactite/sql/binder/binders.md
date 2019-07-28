## Binders overview

Binders are aimed at giving a more fine grained access to
 - [ResultSet read](ResultSetReader.java)
 - [PreparedStatement write](PreparedStatementWriter.java)

from a "column" point of view.

These accesses are given repectiviely through functional interfaces:
 - `ResultSetReader#get(ResultSet resultSet, String columnName)`
 - `PreparedStatementWriter#set(PreparedStatement preparedStatement, int valueIndex, I value)`

Those concepts are merged into the original main class [ParameterBinder](ParameterBinder.java) that implements
those interfaces.


## Predefined binders

Standards and wellknown accessors are defined as constants respectively in:
 - [DefaultResultSetReaders](DefaultResultSetReaders.java)
 - [DefaultPreparedStatementWriters](DefaultPreparedStatementWriters.java)
 - [DefaultParameterBinders](DefaultParameterBinders.java)

You're free to use these constants or not as they are only shortcuts for ResultSet and PreparedStatement methods,
 **except for null handling because it needs some wrapping**.

Some types need conversion, especially standard JDK Date because the JDBC API doesn't take it into account. For that purpose some classes were written to fill this gap:
 - [DateBinder](DateBinder.java)
 - [LocalDateBinder](LocalDateBinder.java)
 - [LocalDateTimeBinder](LocalDateTimeBinder.java)

## FAQ

###### Why using a column name signature in ResultSetReader#get(..), not an index ?

This is done to enforce good practice that allows column order change and gives more readability. 

###### Why using an index signature in PreparedStatementWriter#set(..), not a parameter name ?

This is done to enforce good practice that avoid SQL injection in PreparedStatement.
Upper layers use named parameters to give readability but convert them into PreparedStatement placeholder : `?`


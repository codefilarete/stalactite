package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.query.builder.PseudoTableSQLBuilderFactory;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory;
import org.codefilarete.stalactite.sql.ddl.DDLGenerator;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.GeneratedKeysReader;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;

/**
 * Class that keeps objects necessary to "communicate" with a Database at the SQL language level:
 * - column types for their creation: {@link SqlTypeRegistry} 
 * - column types for their read and write in {@link java.sql.PreparedStatement} and {@link java.sql.ResultSet}: {@link ColumnBinderRegistry}
 * - engines for SQL generation: {@link DDLGenerator} and {@link DMLGenerator}
 * 
 * @author Guillaume Mary
 */
public interface Dialect {
	
	DDLTableGenerator getDdlTableGenerator();
	
	DMLGenerator getDmlGenerator();
	
	WriteOperationFactory getWriteOperationFactory();
	
	ReadOperationFactory getReadOperationFactory();
	
	QuerySQLBuilderFactory getQuerySQLBuilderFactory();
	
	PseudoTableSQLBuilderFactory getUnionSQLBuilderFactory();
	
	SqlTypeRegistry getSqlTypeRegistry();
	
	ColumnBinderRegistry getColumnBinderRegistry();
	
	DMLNameProviderFactory getDmlNameProviderFactory();
	
	int getInOperatorMaxSize();
	
	<I> GeneratedKeysReader<I> buildGeneratedKeysReader(String keyName, Class<I> columnType);
	
	/**
	 * Indicates if this dialect supports what ANSI-SQL terms "row value constructor" syntax, also called tuple syntax.
	 * Basically, does it support syntax like <pre>"... where (FIRST_NAME, LAST_NAME) = ('John', 'Doe')"</pre>.
	 *
	 * @return true if this SQL dialect supports "row value constructor" syntax, false otherwise.
	 */
	boolean supportsTupleCondition();
}

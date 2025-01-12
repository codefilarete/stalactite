package org.codefilarete.stalactite.engine;

import java.util.Set;

import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;

/**
 * 
 * @author Guillaume Mary
 */
public class DatabaseVendorSettings {
	
	/**
	 * Keywords to be escaped in generated SQL 
	 */
	private final Set<String> keyWords;
	
	private final char quotingCharacter;
	
	private final JavaTypeToSqlTypeMapping javaTypeToSqlTypes;
	
	private final ParameterBinderRegistry parameterBinderRegistry;
	
	private final SQLOperationsFactoriesBuilder sqlOperationsFactoriesBuilder;
	
	private final GeneratedKeysReaderFactory generatedKeysReaderFactory;
	
	/**
	 * Maximum number of values for an "in" operator
	 */
	private final int inOperatorMaxSize;
	
	private final boolean supportsTupleCondition;
	
	public DatabaseVendorSettings(Set<String> keyWords,
								  char quotingCharacter,
								  JavaTypeToSqlTypeMapping javaTypeToSqlTypes,
								  ParameterBinderRegistry parameterBinderRegistry,
								  SQLOperationsFactoriesBuilder sqlOperationsFactoriesBuilder,
								  GeneratedKeysReaderFactory generatedKeysReaderFactory,
								  int inOperatorMaxSize,
								  boolean supportsTupleCondition) {
		this.keyWords = keyWords;
		this.quotingCharacter = quotingCharacter;
		this.javaTypeToSqlTypes = javaTypeToSqlTypes;
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.sqlOperationsFactoriesBuilder = sqlOperationsFactoriesBuilder;
		this.generatedKeysReaderFactory = generatedKeysReaderFactory;
		this.inOperatorMaxSize = inOperatorMaxSize;
		this.supportsTupleCondition = supportsTupleCondition;
	}
	
	public Set<String> getKeyWords() {
		return keyWords;
	}
	
	public char getQuotingCharacter() {
		return quotingCharacter;
	}
	
	public JavaTypeToSqlTypeMapping getJavaTypeToSqlTypes() {
		return javaTypeToSqlTypes;
	}
	
	public ParameterBinderRegistry getParameterBinderRegistry() {
		return parameterBinderRegistry;
	}
	
	public SQLOperationsFactoriesBuilder getSqlOperationsFactoriesBuilder() {
		return sqlOperationsFactoriesBuilder;
	}
	
	public GeneratedKeysReaderFactory getGeneratedKeysReaderFactory() {
		return generatedKeysReaderFactory;
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	public boolean supportsTupleCondition() {
		return supportsTupleCondition;
	}
}

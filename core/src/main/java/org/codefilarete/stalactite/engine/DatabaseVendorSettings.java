package org.codefilarete.stalactite.engine;

import java.util.Set;

import org.codefilarete.stalactite.sql.DatabaseSequenceSelectorFactory;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;

/**
 * Generic configuration of a vendor database.
 * This class gives the SQL factories which are necessary to a build a {@link org.codefilarete.stalactite.sql.Dialect}.
 * The class is generic and can be used with any database, user may override it to creates an appropriate one for a particular vendor, and put it
 * close to the  {@link org.codefilarete.stalactite.sql.DialectResolver}.
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
	
	private final DatabaseSequenceSelectorFactory databaseSequenceSelectorFactory;
	
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
								  DatabaseSequenceSelectorFactory databaseSequenceSelectorFactory,
								  int inOperatorMaxSize,
								  boolean supportsTupleCondition) {
		this.keyWords = keyWords;
		this.quotingCharacter = quotingCharacter;
		this.javaTypeToSqlTypes = javaTypeToSqlTypes;
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.sqlOperationsFactoriesBuilder = sqlOperationsFactoriesBuilder;
		this.generatedKeysReaderFactory = generatedKeysReaderFactory;
		this.databaseSequenceSelectorFactory = databaseSequenceSelectorFactory;
		this.inOperatorMaxSize = inOperatorMaxSize;
		this.supportsTupleCondition = supportsTupleCondition;
	}
	
	public Set<String> getKeyWords() {
		return keyWords;
	}
	
	public char getQuoteCharacter() {
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
	
	public DatabaseSequenceSelectorFactory getDatabaseSequenceSelectorFactory() {
		return databaseSequenceSelectorFactory;
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	public boolean supportsTupleCondition() {
		return supportsTupleCondition;
	}
}

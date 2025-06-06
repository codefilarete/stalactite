package org.codefilarete.stalactite.engine;

import java.util.Set;

import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;
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
	
	private final DatabaseSignet compatibility;
	
	/**
	 * Keywords to be escaped in generated SQL 
	 */
	private final Set<String> keywords;
	
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
	
	public DatabaseVendorSettings(DatabaseSignet compatibility,
								  Set<String> keywords,
								  char quotingCharacter,
								  JavaTypeToSqlTypeMapping javaTypeToSqlTypes,
								  ParameterBinderRegistry parameterBinderRegistry,
								  SQLOperationsFactoriesBuilder sqlOperationsFactoriesBuilder,
								  GeneratedKeysReaderFactory generatedKeysReaderFactory,
								  int inOperatorMaxSize,
								  boolean supportsTupleCondition) {
		this.compatibility = compatibility;
		this.keywords = keywords;
		this.quotingCharacter = quotingCharacter;
		this.javaTypeToSqlTypes = javaTypeToSqlTypes;
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.sqlOperationsFactoriesBuilder = sqlOperationsFactoriesBuilder;
		this.generatedKeysReaderFactory = generatedKeysReaderFactory;
		this.inOperatorMaxSize = inOperatorMaxSize;
		this.supportsTupleCondition = supportsTupleCondition;
	}
	
	public DatabaseSignet getCompatibility() {
		return compatibility;
	}
	
	public Set<String> getKeywords() {
		return keywords;
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
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	public boolean supportsTupleCondition() {
		return supportsTupleCondition;
	}
}

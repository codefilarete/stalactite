package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.PostgreSQLDialectResolver.PostgreSQLDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.PostgreSQLParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.PostgreSQLTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.collection.CaseInsensitiveSet;

/**
 * 
 * @author Guillaume Mary
 */
public class PostgreSQLDatabaseSettings extends DatabaseVendorSettings {
	
	public static final PostgreSQLDatabaseSettings POSTGRESQL_9_6 = new PostgreSQLDatabaseSettings();
	
	private PostgreSQLDatabaseSettings() {
		this(new ReadOperationFactory(), new PostgreSQLParameterBinderRegistry());
	}
	
	private PostgreSQLDatabaseSettings(ReadOperationFactory readOperationFactory, PostgreSQLParameterBinderRegistry parameterBinderRegistry) {
		super(new PostgreSQLDatabaseSignet(9, 6),
				Collections.unmodifiableSet(new CaseInsensitiveSet()),
				'"',
				new PostgreSQLTypeMapping(),
				parameterBinderRegistry,
				new SQLOperationsFactoriesBuilder() {
					@Override
					public SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> columnBinderRegistry, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry) {
						return null;
					}
				},
				new DefaultGeneratedKeysReaderFactory(parameterBinderRegistry),
				100,
				true);
	}
}

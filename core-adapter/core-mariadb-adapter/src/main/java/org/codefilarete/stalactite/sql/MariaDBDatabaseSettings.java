package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.MariaDBDialectResolver.MariaDBDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.MariaDBParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.MariaDBTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.collection.CaseInsensitiveSet;
import org.codefilarete.tool.function.Sequence;

/**
 * 
 * @author Guillaume Mary
 */
public class MariaDBDatabaseSettings extends DatabaseVendorSettings {
	
	public static final MariaDBDatabaseSettings MARIADB_10_0 = new MariaDBDatabaseSettings();
	
	private MariaDBDatabaseSettings() {
		this(new ReadOperationFactory(), new MariaDBParameterBinderRegistry());
	}
	
	private MariaDBDatabaseSettings(ReadOperationFactory readOperationFactory, MariaDBParameterBinderRegistry parameterBinderRegistry) {
		super(new MariaDBDatabaseSignet(10, 0),
				Collections.unmodifiableSet(new CaseInsensitiveSet()),
				'"',
				new MariaDBTypeMapping(),
				parameterBinderRegistry,
				new SQLOperationsFactoriesBuilder() {
					@Override
					public SQLOperationsFactories build(ParameterBinderIndex<Column, ParameterBinder> columnBinderRegistry, DMLNameProviderFactory dmlNameProviderFactory, SqlTypeRegistry sqlTypeRegistry) {
						return null;
					}
				},
				new DefaultGeneratedKeysReaderFactory(parameterBinderRegistry),
				new DatabaseSequenceSelectorFactory() {
					@Override
					public Sequence<Long> create(org.codefilarete.stalactite.sql.ddl.structure.Sequence databaseSequence, ConnectionProvider connectionProvider) {
						return null;
					}
				}, 
				100,
				true);
	}
}

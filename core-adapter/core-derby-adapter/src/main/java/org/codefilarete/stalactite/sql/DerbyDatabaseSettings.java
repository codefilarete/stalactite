package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.sql.DerbyDialectResolver.DerbyDatabaseSignet;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.DerbyParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.DerbyTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.collection.CaseInsensitiveSet;
import org.codefilarete.tool.function.Sequence;

/**
 * 
 * @author Guillaume Mary
 */
public class DerbyDatabaseSettings extends DatabaseVendorSettings {
	
	public static final DerbyDatabaseSettings DERBY_10_14 = new DerbyDatabaseSettings();
	
	private DerbyDatabaseSettings() {
		this(new ReadOperationFactory(), new DerbyParameterBinderRegistry());
	}
	
	private DerbyDatabaseSettings(ReadOperationFactory readOperationFactory, DerbyParameterBinderRegistry parameterBinderRegistry) {
		super(new DerbyDatabaseSignet(10, 14),
				Collections.unmodifiableSet(new CaseInsensitiveSet()),
				'"',
				new DerbyTypeMapping(),
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

package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.engine.SQLOperationsFactoriesBuilder;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.SQLiteDialectResolver.SQLiteDatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.SQLiteParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.SQLiteTypeMapping;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.collection.CaseInsensitiveSet;
import org.codefilarete.tool.function.Sequence;

/**
 * 
 * @author Guillaume Mary
 */
public class SQLiteDatabaseSettings extends DatabaseVendorSettings {
	
	public static final SQLiteDatabaseSettings SQLITE_3_45 = new SQLiteDatabaseSettings();
	
	private SQLiteDatabaseSettings() {
		this(new ReadOperationFactory(), new SQLiteParameterBinderRegistry());
	}
	
	private SQLiteDatabaseSettings(ReadOperationFactory readOperationFactory, SQLiteParameterBinderRegistry parameterBinderRegistry) {
		super(new SQLiteDatabaseSignet(3, 45),
				Collections.unmodifiableSet(new CaseInsensitiveSet()),
				'"',
				new SQLiteTypeMapping(),
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

package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.engine.SQLOperationsFactories;
import org.codefilarete.stalactite.sql.GeneratedKeysReaderFactory.DefaultGeneratedKeysReaderFactory;
import org.codefilarete.stalactite.sql.HSQLDBDialect.HSQLDBWriteOperationFactory;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;
import org.codefilarete.stalactite.sql.ddl.HSQLDBDDLTableGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator.NoopSorter;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.binder.HSQLDBParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.HSQLDBTypeMapping;
import org.codefilarete.tool.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public class HSQLDBDialectResolver {
	
	public static class HSQLDB_2_0_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final HSQLDBDialect HSQLDB_DIALECT = new HSQLDBDialect();
		
		private static final DatabaseSignet HSQL_2_0_SIGNET = new DatabaseSignet("HSQL Database Engine", 2, 0);
		
		private static final HSQLDBParameterBinderRegistry PARAMETER_BINDER_REGISTRY = new HSQLDBParameterBinderRegistry();
		
		private static final DatabaseVendorSettings HSQLDB_VENDOR_SETTINGS = new DatabaseVendorSettings(
				Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER)),
				'"',
				new HSQLDBTypeMapping(),
				PARAMETER_BINDER_REGISTRY,
				(parameterBinders, dmlNameProviderFactory, sqlTypeRegistry) -> {
					DMLGenerator dmlGenerator = new DMLGenerator(parameterBinders, NoopSorter.INSTANCE, dmlNameProviderFactory);
					HSQLDBDDLTableGenerator ddlTableGenerator = new HSQLDBDDLTableGenerator(sqlTypeRegistry, dmlNameProviderFactory);
					return new SQLOperationsFactories(new HSQLDBWriteOperationFactory(), new ReadOperationFactory(), dmlGenerator, ddlTableGenerator);
				},
				new DefaultGeneratedKeysReaderFactory(PARAMETER_BINDER_REGISTRY),
				100,
				false
		);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return HSQL_2_0_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return HSQLDB_DIALECT;
		}
		
		@Override
		public DatabaseVendorSettings getVendorSettings() {
			return HSQLDB_VENDOR_SETTINGS;
		}
	}
}

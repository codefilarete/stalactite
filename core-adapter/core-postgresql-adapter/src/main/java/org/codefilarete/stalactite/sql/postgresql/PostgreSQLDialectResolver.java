package org.codefilarete.stalactite.sql.postgresql;


import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectResolver;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

import static org.codefilarete.stalactite.sql.postgresql.PostgreSQLDatabaseSettings.POSTGRESQL_9_6;

/**
 * @author Guillaume Mary
 */
public class PostgreSQLDialectResolver {
	
	public static class PostgreSQL_9_6_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final Dialect POSTGRESQL_DIALECT = PostgreSQLDialectBuilder.defaultPostgreSQLDialect();
		
		@Override
		public DatabaseSignet getCompatibility() {
			return POSTGRESQL_9_6.getCompatibility();
		}
		
		@Override
		public Dialect getDialect() {
			return POSTGRESQL_DIALECT;
		}
		
		@Override
		public DatabaseVendorSettings getVendorSettings() {
			return POSTGRESQL_9_6;
		}
	}
	
	public static class PostgreSQLDatabaseSignet extends DatabaseSignet {
		
		public PostgreSQLDatabaseSignet(int majorVersion, int minorVersion) {
			super("PostgreSQL", majorVersion, minorVersion);
		}
	}
}

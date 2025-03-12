package org.codefilarete.stalactite.sql;


import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

import static org.codefilarete.stalactite.sql.PostgreSQLDatabaseSettings.POSTGRESQL_9_6;

/**
 * @author Guillaume Mary
 */
public class PostgreSQLDialectResolver {
	
	public static class PostgreSQL_9_6_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final PostgreSQLDialect POSTGRESQL_DIALECT = new PostgreSQLDialect();
		
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

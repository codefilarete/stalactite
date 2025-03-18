package org.codefilarete.stalactite.sql;


import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

import static org.codefilarete.stalactite.sql.MariaDBDatabaseSettings.MARIADB_10_0;

/**
 * @author Guillaume Mary
 */
public class MariaDBDialectResolver {
	
	public static class MariaDB_10_0_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final Dialect MARIADB_DIALECT = MariaDBDialectBuilder.defaultMariaDBDialect();
		
		private static final DatabaseSignet MARIADB_10_0_SIGNET = MARIADB_10_0.getCompatibility();
		
		@Override
		public DatabaseSignet getCompatibility() {
			return MARIADB_10_0_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return MARIADB_DIALECT;
		}
		
		@Override
		public DatabaseVendorSettings getVendorSettings() {
			return MARIADB_10_0;
		}
	}
	
	public static class MariaDBDatabaseSignet extends DatabaseSignet {
		
		public MariaDBDatabaseSignet(int majorVersion, int minorVersion) {
			super("MariaDB", majorVersion, minorVersion);
		}
	}
}

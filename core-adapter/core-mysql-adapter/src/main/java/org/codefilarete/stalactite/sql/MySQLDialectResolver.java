package org.codefilarete.stalactite.sql;


import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

import static org.codefilarete.stalactite.sql.MySQLDatabaseSettings.MYSQL_5_6;

/**
 * @author Guillaume Mary
 */
public class MySQLDialectResolver {
	
	public static class MySQL_5_6_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final MySQLDialect MYSQL_DIALECT = new MySQLDialect();
		
		@Override
		public DatabaseSignet getCompatibility() {
			return MYSQL_5_6.getCompatibility();
		}
		
		@Override
		public Dialect getDialect() {
			return MYSQL_DIALECT;
		}
		
		@Override
		public DatabaseVendorSettings getVendorSettings() {
			return MYSQL_5_6;
		}
	}
	
	public static class MySQLDatabaseSignet extends DatabaseSignet {
		
		public MySQLDatabaseSignet(int majorVersion, int minorVersion) {
			super("MySQL", majorVersion, minorVersion);
		}
	}
}

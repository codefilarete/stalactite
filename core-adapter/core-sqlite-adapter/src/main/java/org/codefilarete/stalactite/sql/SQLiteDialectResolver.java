package org.codefilarete.stalactite.sql;


import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

import static org.codefilarete.stalactite.sql.SQLiteDatabaseSettings.SQLITE_3_45;

/**
 * @author Guillaume Mary
 */
public class SQLiteDialectResolver {
	
	public static class SQLite_3_45_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final SQLiteDialect SQLite_DIALECT = new SQLiteDialect();
		
		@Override
		public DatabaseSignet getCompatibility() {
			return SQLITE_3_45.getCompatibility();
		}
		
		@Override
		public Dialect getDialect() {
			return SQLite_DIALECT;
		}
		
		@Override
		public DatabaseVendorSettings getVendorSettings() {
			return SQLITE_3_45;
		}
	}
	
	public static class SQLiteDatabaseSignet extends DatabaseSignet {
		
		public SQLiteDatabaseSignet(int majorVersion, int minorVersion) {
			super("SQLite", majorVersion, minorVersion);
		}
	}
}

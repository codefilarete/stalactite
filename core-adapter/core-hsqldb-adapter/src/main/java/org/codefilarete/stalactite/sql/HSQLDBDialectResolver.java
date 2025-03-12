package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class HSQLDBDialectResolver {
	
	public static class HSQLDB_2_7_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final Dialect HSQLDB_DIALECT = HSQLDBDialectBuilder.defaultHSQLDBDialect();
		
		@Override
		public DatabaseSignet getCompatibility() {
			return HSQLDBDatabaseSettings.HSQLDB_2_7.getCompatibility();
		}
		
		@Override
		public Dialect getDialect() {
			return HSQLDB_DIALECT;
		}
		
		@Override
		public HSQLDBDatabaseSettings getVendorSettings() {
			return HSQLDBDatabaseSettings.HSQLDB_2_7;
		}
	}
	
	static final class HSQLDBDatabaseSignet extends ServiceLoaderDialectResolver.DatabaseSignet {
		
		public HSQLDBDatabaseSignet(int majorVersion, int minorVersion) {
			super("HSQL Database Engine", majorVersion, minorVersion);
		}
	}
}

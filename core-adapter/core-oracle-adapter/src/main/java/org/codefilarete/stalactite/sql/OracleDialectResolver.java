package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

import static org.codefilarete.stalactite.sql.OracleDatabaseSettings.ORACLE_23_0;

/**
 * @author Guillaume Mary
 */
public class OracleDialectResolver {
	
	public static class Oracle_23_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final Dialect ORACLE_DIALECT = OracleDialectBuilder.defaultOracleDialect();
		
		@Override
		public DatabaseSignet getCompatibility() {
			return ORACLE_23_0.getCompatibility();
		}
		
		@Override
		public Dialect getDialect() {
			return ORACLE_DIALECT;
		}
		
		@Override
		public DatabaseVendorSettings getVendorSettings() {
			return ORACLE_23_0;
		}
	}
	
	public static class OracleDatabaseSignet extends DatabaseSignet {
		
		public OracleDatabaseSignet(int majorVersion, int minorVersion) {
			super("Oracle", majorVersion, minorVersion);
		}
	}
}

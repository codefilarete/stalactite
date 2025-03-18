package org.codefilarete.stalactite.sql;


import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.sql.DialectResolver.DialectResolverEntry;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

import static org.codefilarete.stalactite.sql.H2DatabaseSettings.H2_1_4;

/**
 * @author Guillaume Mary
 */
public class H2DialectResolver {
	
	public static class H2_1_4_Entry implements DialectResolverEntry {
		
		private static final Dialect H2_DIALECT = H2DialectBuilder.defaultH2Dialect();
		
		@Override
		public DatabaseSignet getCompatibility() {
			return H2_1_4.getCompatibility();
		}
		
		@Override
		public Dialect getDialect() {
			return H2_DIALECT;
		}
		
		@Override
		public DatabaseVendorSettings getVendorSettings() {
			return H2_1_4;
		}
	}
	
	public static class H2DatabaseSignet extends DatabaseSignet {
		
		H2DatabaseSignet(int majorVersion, int minorVersion) {
			super("H2", majorVersion, minorVersion);
		}
	}
}

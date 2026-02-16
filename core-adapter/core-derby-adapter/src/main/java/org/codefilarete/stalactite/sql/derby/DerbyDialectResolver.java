package org.codefilarete.stalactite.sql.derby;


import org.codefilarete.stalactite.engine.DatabaseVendorSettings;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.DialectResolver;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

import static org.codefilarete.stalactite.sql.derby.DerbyDatabaseSettings.DERBY_10_14;

/**
 * @author Guillaume Mary
 */
public class DerbyDialectResolver {
	
	public static class Derby_10_14_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final Dialect DERBY_DIALECT = DerbyDialectBuilder.defaultDerbyDialect();
		
		@Override
		public DatabaseSignet getCompatibility() {
			return DERBY_10_14.getCompatibility();
		}
		
		@Override
		public Dialect getDialect() {
			return DERBY_DIALECT;
		}
		
		@Override
		public DatabaseVendorSettings getVendorSettings() {
			return DERBY_10_14;
		}
	}
	
	static final class DerbyDatabaseSignet extends DatabaseSignet {
		
		public DerbyDatabaseSignet(int majorVersion, int minorVersion) {
			super("Apache Derby", majorVersion, minorVersion);
		}
	}
}

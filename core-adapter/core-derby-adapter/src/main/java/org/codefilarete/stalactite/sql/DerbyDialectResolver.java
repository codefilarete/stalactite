package org.codefilarete.stalactite.sql;


import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class DerbyDialectResolver {
	
	public static class Derby_10_14_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final DerbyDialect DERBY_DIALECT = new DerbyDialect();
		
		private static final DatabaseSignet DERBY_10_14_SIGNET = new DatabaseSignet("Apache Derby", 10, 14);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return DERBY_10_14_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return DERBY_DIALECT;
		}
	}
}

package org.codefilarete.stalactite.sql;


import org.codefilarete.stalactite.sql.DialectResolver.DialectResolverEntry;
import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class H2DialectResolver {
	
	public static class H2_1_4_Entry implements DialectResolverEntry {
		
		private static final H2Dialect H2_DIALECT = new H2Dialect();
		
		private static final DatabaseSignet H2_1_4_SIGNET = new DatabaseSignet("H2", 1, 4);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return H2_1_4_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return H2_DIALECT;
		}
	}
}

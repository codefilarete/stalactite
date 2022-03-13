package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class HSQLDBDialectResolver {
	
	public static class HSQLDB_2_0_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final HSQLDBDialect HSQLDB_DIALECT = new HSQLDBDialect();
		
		private static final DatabaseSignet HSQL_2_0_SIGNET = new DatabaseSignet("HSQL Database Engine", 2, 0);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return HSQL_2_0_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return HSQLDB_DIALECT;
		}
	}
}

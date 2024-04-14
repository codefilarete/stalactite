package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class OracleDialectResolver {
	
	public static class Oracle_23_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final OracleDialect ORACLE_DIALECT = new OracleDialect();
		
		private static final DatabaseSignet ORACLE_23_SIGNET = new DatabaseSignet("Oracle", 23, 0);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return ORACLE_23_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return ORACLE_DIALECT;
		}
	}
}

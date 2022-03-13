package org.codefilarete.stalactite.sql;


import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class PostgreSQLDialectResolver {
	
	public static class PostgreSQL_9_6_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final PostgreSQLDialect POSTGRESQL_DIALECT = new PostgreSQLDialect();
		
		private static final DatabaseSignet POSTGRESQL_9_6_SIGNET = new DatabaseSignet("PostgreSQL", 9, 6);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return POSTGRESQL_9_6_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return POSTGRESQL_DIALECT;
		}
	}
}

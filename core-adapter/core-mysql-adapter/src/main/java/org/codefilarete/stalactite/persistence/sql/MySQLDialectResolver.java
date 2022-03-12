package org.codefilarete.stalactite.persistence.sql;

import org.codefilarete.stalactite.persistence.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class MySQLDialectResolver {
	
	public static class MySQL_5_6_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final MySQLDialect MYSQL_DIALECT = new MySQLDialect();
		
		private static final DatabaseSignet MYSQL_5_6_SIGNET = new DatabaseSignet("MySQL", 5, 6);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return MYSQL_5_6_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return MYSQL_DIALECT;
		}
	}
}

package org.codefilarete.stalactite.sql;


import org.codefilarete.stalactite.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class SQLiteDialectResolver {
	
	public static class SQLite_3_45_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final SQLiteDialect SQLite_DIALECT = new SQLiteDialect();
		
		private static final DatabaseSignet SQLite_3_45_SIGNET = new DatabaseSignet("SQLite", 3, 45);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return SQLite_3_45_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return SQLite_DIALECT;
		}
	}
}

package org.gama.stalactite.persistence.sql;

import org.gama.stalactite.persistence.sql.ServiceLoaderDialectResolver.DatabaseSignet;

/**
 * @author Guillaume Mary
 */
public class MariaDBDialectResolver {
	
	public static class MariaDB_10_0_Entry implements DialectResolver.DialectResolverEntry {
		
		private static final MariaDBDialect MARIADB_DIALECT = new MariaDBDialect();
		
		private static final DatabaseSignet MARIADB_10_0_SIGNET = new DatabaseSignet("MySQL", 10, 0);
		
		@Override
		public DatabaseSignet getCompatibility() {
			return MARIADB_10_0_SIGNET;
		}
		
		@Override
		public Dialect getDialect() {
			return MARIADB_DIALECT;
		}
	}
}

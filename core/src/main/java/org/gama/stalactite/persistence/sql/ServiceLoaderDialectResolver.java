package org.gama.stalactite.persistence.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.exception.Exceptions;

/**
 * Implementation of {@link DialectResolver} that gets its registered {@link Dialect}s through JVM Service Provider and looks for the most compatible
 * one thanks to a compatibility algorithm.
 * 
 * This class will get available dialects and their compatibility as instances of {@link DialectResolverEntry}, themselves declared by JVM Service
 * Provider. Hence, it is expected that dialect implementors declare them through META-INF/services/org.gama.stalactite.persistence.sql.DialectResolver.DialectResolverEntry
 * file. Then when {@link #determineDialect(Connection)} is invoked, database metadata are compared to compatibility given by entries: only entries
 * whom product name exactly matches database one are kept, then comparing version, the highest dialect among smaller than database one is selected.
 * For example, if database is "A wonderfull database 3.8", and 3 dialects for "A wonderfull database" are present with "3.1", "3.5" and "4.0" versions,
 * then the "3.5" will be selected.
 * 
 * Why such algorithm ? because a dialect is expected to benefit from database features, hence its version should be close to the one of the
 * database that implements the feature, meaning at least equal but not lower : a "4.0" dialect may not be compatible with a "3.0" database. Therefore,
 * only smaller dialect versions are valuable, and among them, we take the closest one to benefit from best features. We also consider that databases
 * are retro-compatible so older dialects are still relevant. 
 * 
 * @author Guillaume Mary
 */
public class ServiceLoaderDialectResolver implements DialectResolver {
	
	@Override
	public Dialect determineDialect(Connection conn) {
		DatabaseSignet databaseSignet = DatabaseSignet.fromMetadata(conn);
		ServiceLoader<DialectResolverEntry> dialects = ServiceLoader.load(DialectResolverEntry.class);
		return determineDialect(dialects, databaseSignet);
	}
	
	Dialect determineDialect(Iterable<? extends DialectResolverEntry> dialects, DatabaseSignet databaseSignet) {
		Nullable<DialectResolverEntry> matchingDialect = Nullable.nullable(giveMatchingEntry(dialects, databaseSignet));
		return matchingDialect.map(DialectResolverEntry::getDialect).getOrThrow(
				() -> new IllegalStateException(
						"Unable to determine dialect to use for database \""
								+ databaseSignet.getProductName()
								+ " " + databaseSignet.getMajorVersion()
								+ "." + databaseSignet.getMinorVersion()
								+ "\" among " + Iterables.collectToList(dialects, o -> "{" + Strings.footPrint(o.getCompatibility(),
																DatabaseSignet::toString) + "}")));
	}
	
	@VisibleForTesting
	@javax.annotation.Nullable
	DialectResolverEntry giveMatchingEntry(Iterable<? extends DialectResolverEntry> dialects, DatabaseSignet databaseSignet) {
		// only dialects that exactly matches database product name are kept
		Set<DialectResolverEntry> databaseDialects = Iterables.stream(dialects)
				.filter(entry -> entry.getCompatibility().getProductName().equals(databaseSignet.getProductName()))
				.collect(Collectors.toSet());
		
		if (databaseDialects.isEmpty()) {
			// no dialect for database, caller will handle that
			return null;
		} else {
			// sorting entries by compatibility versions to ease selection of the highest among the smaller than database version
			// Note: we could have used the stream way of collecting things, but it's a bit less readable
			TreeMap<DatabaseSignet, DialectResolverEntry> dialectPerSortedCompatibility = new TreeMap<>(DatabaseSignet.COMPARATOR);
			databaseDialects.forEach(dialect -> dialectPerSortedCompatibility.merge(dialect.getCompatibility(), dialect, (c1, c2) -> {
				// we use same properties as DatabaseSignet comparator ones since we use a TreeMap based on it 
				String printableSignet = Strings.footPrint(c1.getCompatibility(), DatabaseSignet::toString);
				throw new IllegalStateException("Multiple dialects with same database compatibility found : " + printableSignet);
			}));
			
			// we select the highest dialect among the smaller than database version
			Entry<DatabaseSignet, DialectResolverEntry> foundEntry = dialectPerSortedCompatibility.floorEntry(databaseSignet);
			if (foundEntry == null) {
				return null;
			} else {
				return foundEntry.getValue();
			}
		}
	}
	
	/**
	 * Storage for database product and version.
	 */
	static class DatabaseSignet {
		
		/**
		 * Builds a {@link DatabaseSignet} from a connection to create the datatabase signature from its metadata.
		 * Could be a constructor but would require callers to handle {@link SQLException} which is quite boring, therefore this method handles it
		 * by wrapping it into a {@link RuntimeException}
		 * 
		 * @param connection the connection from which a database signature must be created
		 * @return a new {@link DatabaseSignet}
		 */
		static DatabaseSignet fromMetadata(Connection connection) {
			try {
				DatabaseMetaData databaseMetaData = connection.getMetaData();
				return new DatabaseSignet(databaseMetaData.getDatabaseProductName(), databaseMetaData.getDatabaseMajorVersion(), databaseMetaData.getDatabaseMinorVersion());
			} catch (SQLException e) {
				throw Exceptions.asRuntimeException(e);
			}
		}
		
		static Comparator<DatabaseSignet> COMPARATOR = Comparator
				.comparing(DatabaseSignet::getProductName)
				.thenComparingInt(DatabaseSignet::getMajorVersion)
				.thenComparingInt(DatabaseSignet::getMinorVersion);
		
		private final String productName;
		
		private final int majorVersion;
		
		private final int minorVersion;
		
		/**
		 * Constructor with mandatory elements.
		 * See {@link #fromMetadata(Connection)} to buid one for a database.
		 * 
		 * @param productName database product name, must be strictly equals to the one of database metadata, else detection algorithm will fail
		 * @param majorVersion database product major version, as the one given by database metadata
		 * @param minorVersion database product minor version, as the one given by database metadata
		 * @see #fromMetadata(Connection) 
		 */
		DatabaseSignet(String productName, int majorVersion, int minorVersion) {
			this.productName = productName;
			this.majorVersion = majorVersion;
			this.minorVersion = minorVersion;
		}
		
		public String getProductName() {
			return productName;
		}
		
		public int getMajorVersion() {
			return majorVersion;
		}
		
		public int getMinorVersion() {
			return minorVersion;
		}
		
		/**
		 * Implemented as "product name X.Y". To be used for debug or simple printing.
		 * 
		 * @return "product name X.Y"
		 */
		@Override
		public String toString() {
			return productName + " " + majorVersion + "." + minorVersion;
		}
	}
	
}

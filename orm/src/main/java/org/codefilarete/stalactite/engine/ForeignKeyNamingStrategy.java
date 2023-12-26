package org.codefilarete.stalactite.engine;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;

import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.tool.trace.ModifiableInt;

import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Contract for giving a name to a foreign key
 *
 * @author Guillaume Mary
 */
public interface ForeignKeyNamingStrategy {
	
	String DEFAULT_FOREIGNKEY_PREFIX = "FK_";
	
	Comparator<JoinLink<?, Object>> COLUMN_COMPARATOR = Comparator.comparing(JoinLink::getExpression);
	
	<SOURCETABLE extends Table<SOURCETABLE>, TARGETTABLE extends Table<TARGETTABLE>, ID> String giveName(Key<SOURCETABLE, ID> src, Key<TARGETTABLE, ID> target);
	
	ForeignKeyNamingStrategy HASH = new ForeignKeyNamingStrategy() {
		@Override
		public <SOURCETABLE extends Table<SOURCETABLE>, TARGETTABLE extends Table<TARGETTABLE>, ID> String giveName(Key<SOURCETABLE, ID> src, Key<TARGETTABLE, ID> target) {
			// We ensure a consistent ordering of columns, regardless of the order they were bound.
			ModifiableInt hashCode = new ModifiableInt(src.getTable().getAbsoluteName().hashCode());
			target.getColumns().stream().sorted(COLUMN_COMPARATOR).forEach(joinLink -> {
				hashCode.reset(hashCode.getValue() * 31 + joinLink.getExpression().hashCode());
			});
			return DEFAULT_FOREIGNKEY_PREFIX + Integer.toHexString(hashCode.getValue());
		}
	};
	
	/**
	 * Composed of a prefix, source table name, source column name, target column name.
	 * Must be quite unique to prevent Database such as HSQLDB to yel because key names uniqueness is over a table space, not per table.
	 */
	ForeignKeyNamingStrategy DEFAULT = new ForeignKeyNamingStrategy() {
		@Override
		public <SOURCETABLE extends Table<SOURCETABLE>, TARGETTABLE extends Table<TARGETTABLE>, ID> String giveName(Key<SOURCETABLE, ID> src, Key<TARGETTABLE, ID> target) {
			if (src.getColumns().size() == 1) {
				return DEFAULT_FOREIGNKEY_PREFIX + src.getTable().getName() + "_" + ((Column) first(src.getColumns())).getName()
						+ "_" + target.getTable().getName() + "_" + ((Column) first(target.getColumns())).getName();
			} else {
				return HASH.giveName(src, target);
			}
		}
	};
	
	/**
	 * Generates same name as Hibernate (4.3.7) does. From org.hibernate.mapping.Constraint#generateName(String, Table, Column... columns)
	 */
	ForeignKeyNamingStrategy HIBERNATE = new ForeignKeyNamingStrategy() {
		@Override
		public <SOURCETABLE extends Table<SOURCETABLE>, TARGETTABLE extends Table<TARGETTABLE>, ID> String giveName(Key<SOURCETABLE, ID> src, Key<TARGETTABLE, ID> target) {
			// Use a concatenation that guarantees uniqueness, even if identical names
			// exist between all table and column identifiers.
			StringBuilder sb = new StringBuilder("table`" + src.getTable().getName() + "`");
			
			// We ensure a consistent ordering of columns, regardless of the order they were bound.
			target.getColumns().stream().sorted(COLUMN_COMPARATOR).forEach(column -> sb.append("column`").append(column.getExpression()).append("`"));
			return "FK" + hashName(sb.toString());
		}
	};
	
	/**
	 * Same algorithm as Hibernate (4.3.7), see org.hibernate.mapping.Constraint#hashedName(String)
	 *
	 * @param s the string to be hashed
	 * @return a hashed (MD5) version of the given String, less than 30 characters
	 */
	static String hashName(String s) {
		try {
			@SuppressWarnings("java:S4790" /* No sensitive code related to MD5 usage : only used to hash foreign key name */)
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(s.getBytes());
			byte[] digest = md.digest();
			BigInteger bigInt = new BigInteger(1, digest);
			// By converting to base 35 (full alphanumeric), we guarantee
			// that the length of the name will always be smaller than the 30
			// character identifier restriction enforced by a few dialects.
			return bigInt.toString(35);
		} catch (NoSuchAlgorithmException e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
}

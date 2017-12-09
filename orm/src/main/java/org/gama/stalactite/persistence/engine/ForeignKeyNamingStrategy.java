package org.gama.stalactite.persistence.engine;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.gama.lang.exception.Exceptions;
import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public interface ForeignKeyNamingStrategy {
	
	String DEFAULT_FOREIGNEKEY_PREFIX = "FK_";
	
	String giveName(Column src, Column target);
	
	/**
	 * Composed of a prefix, source table name, source column name, target column name.
	 * Must be quite unique to prevent Database such as HSQLDB to yiel because key names unicity is over a table space, not per table.
	 */
	ForeignKeyNamingStrategy DEFAULT = (src, target) -> DEFAULT_FOREIGNEKEY_PREFIX + src.getTable().getAbsoluteName() + "_" + src.getName() + "_" + target.getName();
	
	ForeignKeyNamingStrategy HASH = (src, target) -> DEFAULT_FOREIGNEKEY_PREFIX
			+ Integer.toHexString(src.getTable().getAbsoluteName().hashCode() * 31 * 31 + src.getName().hashCode() * 31 + target.getName().hashCode());
	
	/** Generates same name as Hibernate (4.3.7) does. From org.hibernate.mapping.Constraint#generateName(String, Table, Column... columns) */
	ForeignKeyNamingStrategy HIBERNATE = (src, target) -> {
		StringBuilder sb = new StringBuilder( "table`" + src.getTable().getName() + "`" );
		sb.append( "column`" + src.getName() + "`" );
		return "FK" + hashedName(sb.toString());
	};
	
	/**
	 * Same algorithm as Hibernate (4.3.7), see org.hibernate.mapping.Constraint#hashedName(String)
	 * @param s the string to be hashed
	 * @return a hashed (MD5) version of the given String, less than 30 characters
	 */
	static String hashedName(String s) {
		try {
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

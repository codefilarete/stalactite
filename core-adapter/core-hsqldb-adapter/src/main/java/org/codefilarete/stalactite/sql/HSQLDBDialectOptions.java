package org.codefilarete.stalactite.sql;

/**
 * Dialect options dedicated to HSQLDB
 * 
 * @author Guillaume Mary
 */
public class HSQLDBDialectOptions extends DialectOptions {
	
	public static HSQLDBDialectOptions defaultHSQLDBOptions() {
		return new HSQLDBDialectOptions();
	}
	
	private final OptionalSetting<Boolean> useExtendedKeywordSet = new OptionalSetting<>();
	
	public OptionalSetting<Boolean> getUseExtendedKeywordSet() {
		return useExtendedKeywordSet;
	}
	
	public HSQLDBDialectOptions setUseExtendedKeywordSet(boolean useExtendedKeywordSet) {
		this.useExtendedKeywordSet.set(useExtendedKeywordSet);
		return this;
	}
	
	public HSQLDBDialectOptions useExtendedKeywordSet() {
		return setUseExtendedKeywordSet(true);
	}
}

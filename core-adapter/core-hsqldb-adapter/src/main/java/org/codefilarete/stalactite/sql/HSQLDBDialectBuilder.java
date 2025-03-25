package org.codefilarete.stalactite.sql;

import java.util.Collections;

import org.codefilarete.stalactite.engine.DialectBuilder;
import org.codefilarete.tool.collection.CaseInsensitiveSet;

import static org.codefilarete.stalactite.sql.HSQLDBDatabaseSettings.EXTENDED_KEYWORDS;

/**
 * {@link Dialect} builder dedicated to HSQLDB
 * 
 * @author Guillaume Mary
 */
public class HSQLDBDialectBuilder extends DialectBuilder {
	
	public static Dialect defaultHSQLDBDialect() {
		return new HSQLDBDialectBuilder().build();
	}
	
	public HSQLDBDialectBuilder() {
		this(DialectOptions.noOptions());
	}
	
	public HSQLDBDialectBuilder(DialectOptions dialectOptions) {
		super(HSQLDBDatabaseSettings.HSQLDB_2_7, dialectOptions);
	}
	
	@Override
	protected DMLNameProviderFactory buildDmlNameProviderFactory() {
		if (super.dialectOptions instanceof HSQLDBDialectOptions
				&& ((HSQLDBDialectOptions) super.dialectOptions).getUseExtendedKeywordSet().getOrDefault(false)) {
			return tableAliaser ->
					new QuotingKeywordsDMLNameProvider(tableAliaser, Collections.unmodifiableSet(new CaseInsensitiveSet(EXTENDED_KEYWORDS)));
		} else {
			return super.buildDmlNameProviderFactory();
		}
	}
}

package org.codefilarete.stalactite.dsl.idpolicy;

import org.codefilarete.stalactite.mapping.id.sequence.DatabaseSequenceSettings;

/**
 * Identifier pôlicy support for Database sequence (per entity)
 */
public class DatabaseSequenceIdentifierPolicySupport implements BeforeInsertIdentifierPolicy<Long> {
	
	private final DatabaseSequenceNamingStrategy databaseSequenceNamingStrategy;
	private final DatabaseSequenceSettings databaseSequenceSettings;
	
	public DatabaseSequenceIdentifierPolicySupport() {
		this(DatabaseSequenceNamingStrategy.DEFAULT);
	}
	
	public DatabaseSequenceIdentifierPolicySupport(DatabaseSequenceNamingStrategy databaseSequenceNamingStrategy) {
		this(databaseSequenceNamingStrategy, new DatabaseSequenceSettings(1, 1));
	}
	
	public DatabaseSequenceIdentifierPolicySupport(DatabaseSequenceNamingStrategy databaseSequenceNamingStrategy, DatabaseSequenceSettings databaseSequenceSettings) {
		this.databaseSequenceNamingStrategy = databaseSequenceNamingStrategy;
		this.databaseSequenceSettings = databaseSequenceSettings;
	}
	
	public DatabaseSequenceNamingStrategy getDatabaseSequenceNamingStrategy() {
		return databaseSequenceNamingStrategy;
	}
	
	public DatabaseSequenceSettings getDatabaseSequenceSettings() {
		return databaseSequenceSettings;
	}
}

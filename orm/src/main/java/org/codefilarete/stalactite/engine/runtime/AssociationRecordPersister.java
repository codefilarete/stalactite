package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;

/**
 * Persister dedicated to record of association table (case of one-to-many association without owning column on target side).
 * Please note whereas 2 DTO exists for indexed and non indexed one-to-many association, there's no 2 dedicated persister because both cases can
 * be completed with some generics, and index is not used by persister class (it is {@link ClassMapping}'s job)
 * 
 * @author Guillaume Mary
 */
public class AssociationRecordPersister<C extends AssociationRecord, T extends AssociationTable<T, ?, ?, ?, ?>> extends Persister<C, C, T> {
	
	public AssociationRecordPersister(
			ClassMapping<C, C, T> mappingStrategy,
			Dialect dialect,
			ConnectionConfiguration connectionConfiguration) {
		super(mappingStrategy, dialect, connectionConfiguration);
	}
	
}

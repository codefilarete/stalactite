package org.codefilarete.stalactite.sql;

import org.codefilarete.stalactite.sql.ddl.structure.Sequence;

/**
 * Factory contract for a database sequence reader.
 * Must be quite generic because some database vendor don't support sequence. Thought it must be mimicked (through a table storage for example)
 * 
 * @author Guillaume Mary
 */
public interface DatabaseSequenceSelectorFactory {
	
	org.codefilarete.tool.function.Sequence<Long> create(Sequence databaseSequence, ConnectionProvider connectionProvider);
}
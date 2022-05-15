package org.codefilarete.stalactite.engine.runtime.load;

/**
 * Marking interface for equivalence of {@link JoinNode} that consumes {@link org.codefilarete.stalactite.sql.result.Row} in {@link EntityTreeInflater}
 */
interface JoinRowConsumer {
	
	interface ForkJoinRowConsumer extends JoinRowConsumer {
		
		JoinRowConsumer giveNextConsumer();
		
	}
}

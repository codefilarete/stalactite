package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * Marking interface for consumers of {@link org.codefilarete.stalactite.sql.result.Row} during result set inflating phase
 * ({@link EntityTreeInflater}). This is an internal interface and mechanism not expected to be implemented by external
 * code. If you need to access read data during inflating phase, please use {@link AbstractJoinNode#getConsumptionListener()}
 * (available through {@link EntityJoinTree#addPassiveJoin(String, Key, Key, String, JoinType, Set, EntityTreeJoinNodeConsumptionListener, boolean)}
 * and other join methods) mechanism instead of this.
 * 
 * Each kind of join implements its own {@link JoinRowConsumer} and {@link EntityTreeInflater} is aware of them by
 * calling sub-class-dedicated methods because there's no common point between them to make an abstract method.
 */
interface JoinRowConsumer {
	
	interface RootJoinRowConsumer<C> extends JoinRowConsumer {
		
		C createRootInstance(Row row, TreeInflationContext context);
		
	}
	
	interface ForkJoinRowConsumer extends JoinRowConsumer {
		
		JoinRowConsumer giveNextConsumer();
		
	}
}

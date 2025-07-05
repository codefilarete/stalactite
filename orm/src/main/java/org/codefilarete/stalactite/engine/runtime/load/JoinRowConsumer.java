package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

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
public interface JoinRowConsumer {
	
	JoinNode<?, ?> getNode();
	
	interface RootJoinRowConsumer<C> extends JoinRowConsumer {
		
		C createRootInstance(ColumnedRow row, TreeInflationContext context);
		
	}
	
	/**
	 * Interface to implement when a {@link JoinRowConsumer} needs to provide the next join consumer (among the ones he owns): in case of polymorphism,
	 * we need to choose the right next branch that fills the created entity. This interface is made to provide the next sub-consumer for the next
	 * iteration.
	 * @author Guillaume Mary
	 */
	interface ForkJoinRowConsumer extends JoinRowConsumer {
		
		JoinRowConsumer giveNextConsumer();
		
	}
	
	/**
	 * Interface to implement when a {@link RootJoinRowConsumer} needs to skip some joins he owns: by default {@link EntityTreeInflater} iterates
	 * over all consumers returned by a {@link RootJoinRowConsumer} but, in case of polymorphism, we need to skip the branches that are not implied
	 * in the process of instance creation. This interface is made to provide the sub-consumers to exclude from next iteration.
	 * @author Guillaume Mary
	 */
	interface ExcludingJoinRowConsumer<C> extends RootJoinRowConsumer<C> {
		
		//deadBranches
		Set<JoinRowConsumer> giveExcludedConsumers();
		
	}
}

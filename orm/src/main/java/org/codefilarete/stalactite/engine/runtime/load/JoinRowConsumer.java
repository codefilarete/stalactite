package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

/**
 * The JoinRowConsumer interface defines a contract for consuming rows from a database result set
 * as part of an object graph construction process. This mechanism is used to handle hierarchical
 * joins and populate corresponding object nodes in an entity tree during the processing.
 */
public interface JoinRowConsumer {
	
	JoinNode<?, ?> getNode();
	
	default void beforeRowConsumption(TreeInflationContext context) {
		
	}
	
	default void afterRowConsumption(TreeInflationContext context) {
		
	}
	
	interface RootJoinRowConsumer<C, I> extends JoinRowConsumer {
		
		EntityReference<C, I> createRootInstance(ColumnedRow row, TreeInflationContext context);
		
	}
	
	/**
	 * A container class that holds a reference to an entity and its corresponding identifier.
	 * Made to avoid relying on entity equals/hashcode but on identifier equality, because
	 * that's one of Stalactite principles: avoid implementing equals/hashCode on entities, but
	 * make it mandatory for identifier because it's more logical for them, whereas entity
	 * equality may rely on business rules and ORM shouldn't force it to base it on identifier.
	 *
	 * @param <C> the type of the entity
	 * @param <I> the type of the identifier associated with the entity
	 */
	class EntityReference<C, I> {
		
		private final C entity;
		private final I identifier;
		
		public EntityReference(C entity, I identifier) {
			this.entity = entity;
			this.identifier = identifier;
		}
		
		public C getEntity() {
			return entity;
		}
		
		public I getIdentifier() {
			return identifier;
		}
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
	interface ExcludingJoinRowConsumer<C, I> extends RootJoinRowConsumer<C, I> {
		
		//deadBranches
		Set<JoinRowConsumer> giveExcludedConsumers();
		
	}
}

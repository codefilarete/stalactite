package org.codefilarete.stalactite.dsl;

import java.util.NoSuchElementException;
import javax.annotation.Nullable;

import org.codefilarete.tool.collection.ReadOnlyIterator;

/**
 * Technical sharing of inheritance abilities of {@link org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration}
 * and {@link org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration}
 * 
 * @param <C>
 * @author Guillaume Mary
 */
public interface MappableSuperClassConfiguration<C> {
	
	/**
	 * 
	 * @return the configuration of a parent type
	 */
	@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
	@Nullable
	MappableSuperClassConfiguration<? super C> getMappedSuperClassConfiguration();
	
	/**
	 * @return an iterable for all inheritance configurations, including this
	 */
	default Iterable<? extends MappableSuperClassConfiguration<?>> inheritanceIterable() {
		
		return () -> new ReadOnlyIterator<MappableSuperClassConfiguration<?>>() {
			
			private MappableSuperClassConfiguration<?> next = MappableSuperClassConfiguration.this;
			
			@Override
			public boolean hasNext() {
				return next != null;
			}
			
			@Override
			public MappableSuperClassConfiguration<?> next() {
				if (!hasNext()) {
					// comply with next() method contract
					throw new NoSuchElementException();
				}
				MappableSuperClassConfiguration<?> result = this.next;
				this.next = this.next.getMappedSuperClassConfiguration();
				return result;
			}
		};
	}
}

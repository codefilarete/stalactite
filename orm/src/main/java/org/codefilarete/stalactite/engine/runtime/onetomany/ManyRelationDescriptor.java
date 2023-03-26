package org.codefilarete.stalactite.engine.runtime.onetomany;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.bean.Objects;

/**
 * Container to store information of a one-to-many relation
 * 
 * @author Guillaume Mary
 */
public class ManyRelationDescriptor<I, O, C extends Collection<O>> {
	
	/** Empty setter for applying source entity to target entity (reverse side) */
	private static final BiConsumer NOOP_REVERSE_SETTER = (o, i) -> {};
	
	private final Function<I, C> collectionGetter;
	
	private final BiConsumer<I, C> collectionSetter;
	
	private final Supplier<C> collectionFactory;
	
	private final BiConsumer<O, I> reverseSetter;
	
	private final BeanRelationFixer<I, O> relationFixer;
	
	/**
	 * @param collectionGetter collection accessor
	 * @param collectionSetter collection setter
	 * @param collectionFactory collection factory
	 * @param reverseSetter
	 */
	public ManyRelationDescriptor(Function<I, C> collectionGetter,
								  BiConsumer<I, C> collectionSetter,
								  Supplier<C> collectionFactory,
								  @Nullable BiConsumer<O, I> reverseSetter) {
		this.collectionGetter = collectionGetter;
		this.collectionSetter = collectionSetter;
		this.collectionFactory = collectionFactory;
		this.reverseSetter = reverseSetter;
		
		// configuring select for fetching relation
		relationFixer = BeanRelationFixer.of(
				this.collectionSetter,
				this.collectionGetter,
				this.collectionFactory,
				Objects.preventNull(reverseSetter, NOOP_REVERSE_SETTER));
	}
	
	public Function<I, C> getCollectionGetter() {
		return collectionGetter;
	}
	
	public BiConsumer<I, C> getCollectionSetter() {
		return collectionSetter;
	}
	
	public Supplier<C> getCollectionFactory() {
		return collectionFactory;
	}
	
	/**
	 * Gives the setter for source bean on the owning side. 
	 *
	 * @return null if no setter given at construction time
	 */
	@Nullable
	public BiConsumer<O, I> getReverseSetter() {
		return reverseSetter;
	}
	
	public BeanRelationFixer<I, O> getRelationFixer() {
		return relationFixer;
	}
}

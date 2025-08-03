package org.codefilarete.stalactite.engine.runtime.onetomany;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.bean.Objects;

/**
 * Container to store information of a one-to-many relation
 * 
 * @author Guillaume Mary
 */
public class ManyRelationDescriptor<I, O, C extends Collection<O>> {
	
	/** Empty setter for applying source entity to target entity (reverse side) */
	protected static final BiConsumer NOOP_REVERSE_SETTER = (o, i) -> {};
	
	private final Accessor<I, C> collectionProvider;
	
	private final BiConsumer<I, C> collectionSetter;
	
	private final Supplier<C> collectionFactory;
	
	private final BiConsumer<O, I> reverseSetter;
	
	protected BeanRelationFixer<I, O> relationFixer;
	
	/**
	 * @param collectionProvider collection accessor
	 * @param collectionSetter collection setter
	 * @param collectionFactory collection factory
	 * @param reverseSetter
	 */
	public ManyRelationDescriptor(Accessor<I, C> collectionProvider,
								  BiConsumer<I, C> collectionSetter,
								  Supplier<C> collectionFactory,
								  @Nullable BiConsumer<O, I> reverseSetter) {
		this(collectionProvider,
				collectionSetter,
				collectionFactory,
				reverseSetter,
				BeanRelationFixer.of(collectionSetter, collectionProvider::get, collectionFactory, Objects.preventNull(reverseSetter, (BiConsumer<O, I>) NOOP_REVERSE_SETTER))
		);
	}
	
	public ManyRelationDescriptor(Accessor<I, C> collectionProvider,
								  BiConsumer<I, C> collectionSetter,
								  Supplier<C> collectionFactory,
								  BiConsumer<O, I> reverseSetter,
								  BeanRelationFixer<I, O> relationFixer) {
		this.collectionProvider = collectionProvider;
		this.collectionSetter = collectionSetter;
		this.collectionFactory = collectionFactory;
		this.reverseSetter = reverseSetter;
		this.relationFixer = relationFixer;
	}

	public Accessor<I, C> getCollectionProvider() {
		return collectionProvider;
	}
	
	public Function<I, C> getCollectionGetter() {
		return collectionProvider::get;
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

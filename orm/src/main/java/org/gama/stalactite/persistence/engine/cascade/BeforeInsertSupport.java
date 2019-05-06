package org.gama.stalactite.persistence.engine.cascade;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Predicates;
import org.gama.stalactite.persistence.engine.listening.InsertListener;

/**
 * @author Guillaume Mary
 */
public class BeforeInsertSupport<TRIGGER, TARGET> implements InsertListener<TRIGGER> {
	
	private final Consumer<Iterable<TARGET>> beforeInsertAction;
	
	private final Function<TRIGGER, TARGET> targetProvider;
	
	private final Predicate<TARGET> targetFilter;
	
	public BeforeInsertSupport(Consumer<Iterable<TARGET>> beforeInsertAction, Function<TRIGGER, TARGET> targetProvider) {
		this(beforeInsertAction, targetProvider, Predicates.acceptAll());
	}
	
	public BeforeInsertSupport(Consumer<Iterable<TARGET>> beforeInsertAction, Function<TRIGGER, TARGET> targetProvider, Predicate<TARGET> targetFilter) {
		this.beforeInsertAction = beforeInsertAction;
		this.targetProvider = targetProvider;
		this.targetFilter = targetFilter;
	}

	@Override
	public void beforeInsert(Iterable<? extends TRIGGER> entities) {
		beforeInsertAction.accept(Iterables.stream(entities).map(targetProvider).filter(targetFilter).collect(Collectors.toList()));
	}
}

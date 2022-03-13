package org.codefilarete.stalactite.engine.cascade;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Predicates;
import org.codefilarete.stalactite.engine.listener.InsertListener;

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
		beforeInsertAction.accept(Iterables.stream(entities).map(targetProvider).filter(targetFilter).collect(Collectors.toSet()));
	}
}

package org.codefilarete.stalactite.persistence.engine.cascade;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Predicates;
import org.codefilarete.stalactite.persistence.engine.listening.UpdateListener;

/**
 * @author Guillaume Mary
 */
public class BeforeUpdateSupport<TRIGGER, TARGET> implements UpdateListener<TRIGGER> {
	
	private final BiConsumer<Iterable<Duo<TARGET, TARGET>>, Boolean> beforeUpdateAction;
	
	private final Function<TRIGGER, TARGET> targetProvider;
	
	private final Predicate<Duo<TARGET, TARGET>> targetFilter;
	
	public BeforeUpdateSupport(BiConsumer<Iterable<Duo<TARGET, TARGET>>, Boolean> beforeUpdateAction, Function<TRIGGER, TARGET> targetProvider) {
		this(beforeUpdateAction, targetProvider, Predicates.acceptAll());
	}
	
	public BeforeUpdateSupport(BiConsumer<Iterable<Duo<TARGET, TARGET>>, Boolean> beforeUpdateAction, Function<TRIGGER, TARGET> targetProvider, Predicate<Duo<TARGET, TARGET>> targetFilter) {
		this.beforeUpdateAction = beforeUpdateAction;
		this.targetProvider = targetProvider;
		this.targetFilter = targetFilter;
	}
	
	@Override
	public void beforeUpdate(Iterable<? extends Duo<? extends TRIGGER, ? extends TRIGGER>> entities, boolean allColumnsStatement) {
		beforeUpdateAction.accept(Iterables.stream(entities)
						.map(e -> new Duo<>(targetProvider.apply(e.getLeft()), targetProvider.apply(e.getRight())))
						.filter(targetFilter).collect(Collectors.toList()),
				allColumnsStatement);
	}
}

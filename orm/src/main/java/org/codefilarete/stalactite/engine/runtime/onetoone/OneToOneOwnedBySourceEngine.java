package org.codefilarete.stalactite.engine.runtime.onetoone;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteByIdSupport;
import org.codefilarete.stalactite.engine.cascade.AfterDeleteSupport;
import org.codefilarete.stalactite.engine.cascade.BeforeInsertSupport;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.mapping.Mapping.ShadowColumnValueProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Functions.NullProofFunction;
import org.codefilarete.tool.function.Predicates;

import static org.codefilarete.tool.function.Predicates.not;

public class OneToOneOwnedBySourceEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	extends AbstractOneToOneEngine<SRC, TRGT, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> {
	
	private final ShadowColumnValueProvider<SRC, LEFTTABLE> foreignKeyValueProvider;
	
	public OneToOneOwnedBySourceEngine(ConfiguredPersister<SRC, SRCID> sourcePersister,
									   ConfiguredPersister<TRGT, TRGTID> targetPersister,
									   Accessor<SRC, TRGT> targetAccessor,
									   Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> keyColumnsMapping) {
		super(sourcePersister, targetPersister, targetAccessor, keyColumnsMapping);
		// whatever kind of relation maintenance mode asked, we have to insert and update source-to-target link, because we are in relation-owned-by-source
		Function<SRC, TRGTID> targetIdProvider = src -> {
			TRGT trgt = targetAccessor.get(src);
			return trgt == null ? null : targetPersister.getMapping().getId(trgt);
		};
		foreignKeyValueProvider = new ShadowColumnValueProvider<SRC, LEFTTABLE>() {
			@Override
			public Set<Column<LEFTTABLE, ?>> getColumns() {
				return new HashSet<>(keyColumnsMapping.keySet());
			}
			
			@Override
			public Map<Column<LEFTTABLE, ?>, Object> giveValue(SRC bean) {
				TRGTID trgtid = targetIdProvider.apply(bean);
				Map<Column<RIGHTTABLE, ?>, Object> columnValues = targetPersister.getMapping().getIdMapping().<RIGHTTABLE>getIdentifierAssembler().getColumnValues(trgtid);
				return Maps.innerJoinOnValuesAndKeys(keyColumnsMapping, columnValues);
			}
		};
	}
	
	@Override
	public void addInsertCascade() {
		sourcePersister.<LEFTTABLE>getMapping().addShadowColumnInsert(foreignKeyValueProvider);
		// adding cascade treatment: before source insert, target is inserted to comply with foreign key constraint
		sourcePersister.addInsertListener(new BeforeInsertSupport<>(targetPersister::persist, targetAccessor::get, Objects::nonNull));
	}
	
	@Override
	public void addUpdateCascade(boolean orphanRemoval) {
		super.addUpdateCascade(orphanRemoval);
		sourcePersister.<LEFTTABLE>getMapping().addShadowColumnUpdate(foreignKeyValueProvider);
		// adding cascade treatment
		// - insert non-persisted target instances to fulfill foreign key requirement
		Function<SRC, TRGT> targetProviderAsFunction = new NullProofFunction<>(targetAccessor::get);
		sourcePersister.addUpdateListener(new UpdateListener<SRC>() {
			
			private final Predicate<TRGT> newInstancePredicate = Predicates.<TRGT>predicate(Objects::nonNull).and(targetPersister.getMapping()::isNew);
			
			@Override
			public void beforeUpdate(Iterable<? extends Duo<SRC, SRC>> payloads, boolean allColumnsStatement) {
				// we only insert new instances
				targetPersister.insert(Iterables.stream(payloads).map(duo -> targetProviderAsFunction.apply(duo.getLeft()))
						.filter(newInstancePredicate).collect(Collectors.toSet()));
			}
		});
		// - after source update, target is updated too
		sourcePersister.addUpdateListener(new UpdateListener<SRC>() {
			
			@Override
			public void afterUpdate(Iterable<? extends Duo<SRC, SRC>> payloads, boolean allColumnsStatement) {
				List<Duo<TRGT, TRGT>> targetsToUpdate = Iterables.collect(payloads,
						// targets of nullified relations don't need to be updated 
						e -> getTarget(e.getLeft()) != null,
						e -> getTargets(e.getLeft(), e.getRight()),
						ArrayList::new);
				targetPersister.update(targetsToUpdate, allColumnsStatement);
			}
			
			private Duo<TRGT, TRGT> getTargets(SRC modifiedTrigger, SRC unmodifiedTrigger) {
				return new Duo<>(getTarget(modifiedTrigger), getTarget(unmodifiedTrigger));
			}
			
			private TRGT getTarget(SRC src) {
				return targetAccessor.get(src);
			}
		});
	}
	
	@Override
	public void addDeleteCascade(boolean orphanRemoval) {
		if (orphanRemoval) {
			// adding cascade treatment: target is deleted after source deletion (because of foreign key constraint)
			Predicate<TRGT> deletionPredicate = ((Predicate<TRGT>) Objects::nonNull).and(not(targetPersister.getMapping().getIdMapping()::isNew));
			sourcePersister.addDeleteListener(new AfterDeleteSupport<>(targetPersister::delete, targetAccessor::get, deletionPredicate));
			// we add the deleteById event since we suppose that if delete is required then there's no reason that rough delete is not
			sourcePersister.addDeleteByIdListener(new AfterDeleteByIdSupport<>(targetPersister::delete, targetAccessor::get, deletionPredicate));
		} // else : no target entities deletion asked (no delete orphan) : nothing more to do than deleting the source entity
	}
	
	public void addForeignKeyMaintainer() {
		sourcePersister.<LEFTTABLE>getMapping().addShadowColumnInsert(foreignKeyValueProvider);
		sourcePersister.<LEFTTABLE>getMapping().addShadowColumnUpdate(foreignKeyValueProvider);
	}
}

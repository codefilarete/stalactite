package org.codefilarete.stalactite.engine.configurer.resolver.elementcollection;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Reflections;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.tool.bean.Objects.preventNull;

public class AggregateElementCollectionAppender {
	
	private final ElementCollectionResolver elementCollectionResolver;
	
	public AggregateElementCollectionAppender(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.elementCollectionResolver = new ElementCollectionResolver(dialect, connectionConfiguration);
	}
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>>
	void append(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                     ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, SRCTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> resolvedRelation,
	                     AssemblyPoint<SRC, SRCID, TRGT, SRCTABLE> assemblyPawn) {
		
		ElementCollectionResolver.ElementRecordPersister<TRGT, SRCID, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> collectionPersister = elementCollectionResolver.resolve(resolvedRelation, assemblyPawn.getRelationOwnerPersister());
		
		// select management
		Supplier<S> collectionFactory = preventNull(
				resolvedRelation.getComponentFactory(),
				Reflections.giveCollectionFactory(resolvedRelation.getCollectionType()));
		
		// a particular collection fixer that gets raw values (elements) from ElementRecord
		// because elementRecordPersister manages ElementRecord, so it gives them as input of the relation,
		// hence an adaption is needed to "convert" it.
		// Note that this code is wrongly typed: the relationFixer should be of <SRC, C> to access the property, whereas it is typed with
		// ElementRecord<TRGT, I> to fulfill the adapter argument. There's a kind of magic here that make it works (generics type erasure, and wrong
		// ofAdapter(..) type deduction by compiler to match the relationFixer variable.
		BeanRelationFixer<SRC, ElementRecord<TRGT, SRCID>> relationFixer = BeanRelationFixer.ofAdapter(
				resolvedRelation.getAccessor(),
				collectionFactory,
				(bean, input, collection) -> collection.add(input.getElement()));	// element value is taken from ElementRecord
		
		
		rootPersister.getEntityJoinTree().addRelationJoin(
				assemblyPawn.getParentJoinPoint(),
				new EntityMappingAdapter<>(collectionPersister.getMapping()),
				resolvedRelation.getAccessor(),
				resolvedRelation.getJoin().getLeftKey(),
				resolvedRelation.getJoin().getRightKey(),
				null,
				OUTER,
				relationFixer,
				Collections.emptySet(),
				null);
		
	}
}

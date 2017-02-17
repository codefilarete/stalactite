package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.Reflections;
import org.gama.lang.bean.FieldIterator;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.Accessors;
import org.gama.reflection.IMutator;
import org.gama.reflection.PropertyAccessor;
import org.gama.spy.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.CascadeOption.CascadeType;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterInsertCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCascader;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteRoughlyCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteRoughlyCollectionCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.IInsertListener;
import org.gama.stalactite.persistence.engine.listening.NoopInsertListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer;
import org.gama.stalactite.persistence.id.IdentifiedCollectionDiffer.Diff;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.id.manager.StatefullIdentifier;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class FluentMappingBuilder<T extends Identified, I extends StatefullIdentifier> implements IFluentMappingBuilder<T, I> {
	
	public enum IdentifierPolicy {
		ALREADY_ASSIGNED,
		BEFORE_INSERT,
		AFTER_INSERT
	}
	
	/**
	 * Will start a {@link FluentMappingBuilder} for a given class which will target a table that as the class name.
	 *
	 * @param persistedClass the class to be persisted by the {@link ClassMappingStrategy} that will be created by {@link #build(Dialect)}
	 * @param identifierClass the class of the identifier
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentMappingBuilder}
	 */
	public static <T extends Identified, I extends StatefullIdentifier> FluentMappingBuilder<T, I> from(Class<T> persistedClass,
																										Class<I> identifierClass) {
		return from(persistedClass, identifierClass, new Table(persistedClass.getSimpleName()));
	}
	
	/**
	 * Will start a {@link FluentMappingBuilder} for a given class and a given target table.
	 *
	 * @param persistedClass the class to be persisted by the {@link ClassMappingStrategy} that will be created by {@link #build(Dialect)}
	 * @param identifierClass the class of the identifier
	 * @param table the table which will store instances of the persistedClass
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentMappingBuilder}
	 */
	public static <T extends Identified, I extends StatefullIdentifier> FluentMappingBuilder<T, I> from(Class<T> persistedClass,
																										Class<I> identifierClass, Table table) {
		return new FluentMappingBuilder<>(persistedClass, table);
	}
	
	private final Class<T> persistedClass;
	
	private final Table table;
	
	private List<Linkage> mapping;
	
	private IdentifierInsertionManager<T, I> identifierInsertionManager;
	
	private PropertyAccessor<T, I> identifierAccessor;
	
	private final MethodReferenceCapturer<T> spy;
	
	private List<CascadeOne<T, ? extends Identified, ? extends StatefullIdentifier>> cascadeOnes = new ArrayList<>();
	
	private CascadeMany<T, ? extends Identified, ? extends StatefullIdentifier, ? extends Collection> cascadeMany;
	
	private Collection<Inset<T, ?>> insets = new ArrayList<>();
	
	public FluentMappingBuilder(Class<T> persistedClass, Table table) {
		this.persistedClass = persistedClass;
		this.table = table;
		this.mapping = new ArrayList<>();
		
		// Code enhancer for creation of a proxy that will support functions invocations
		this.spy = new MethodReferenceCapturer<>(persistedClass);
	}
	
	public Table getTable() {
		return table;
	}
	
	private Method captureLambdaMethod(Function<T, ?> function) {
		return this.spy.capture(function);
	}
	
	private <O> Method captureLambdaMethod(BiConsumer<T, O> function) {
		return this.spy.capture(function);
	}
	
	@Override
	public <O> IFluentMappingBuilderColumnOptions<T, I> add(BiConsumer<T, O> function) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Reflections.propertyType(method);
		String columnName = Accessors.propertyName(method);
		return add(method, columnName, columnType);
	}
	
	@Override
	public IFluentMappingBuilderColumnOptions<T, I> add(Function<T, ?> function) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Reflections.propertyType(method);
		String columnName = Accessors.propertyName(method);
		return add(method, columnName, columnType);
	}
	
	@Override
	public IFluentMappingBuilderColumnOptions<T, I> add(Function<T, ?> function, String columnName) {
		Method method = captureLambdaMethod(function);
		Class<?> columnType = Reflections.propertyType(method);
		return add(method, columnName, columnType);
	}
	
	private IFluentMappingBuilderColumnOptions<T, I> add(Method method, String columnName, Class<?> columnType) {
		PropertyAccessor<T, I> propertyAccessor = PropertyAccessor.of(method);
		Column newColumn = addMapping(columnName, columnType, propertyAccessor);
		return new Decorator<>(ColumnOptions.class).decorate(this, (Class<IFluentMappingBuilderColumnOptions<T, I>>) (Class)
				IFluentMappingBuilderColumnOptions.class, identifierPolicy -> {
			if (FluentMappingBuilder.this.identifierAccessor != null) {
				throw new IllegalArgumentException("Identifier is already defined by " + identifierAccessor.getAccessor());
			}
			switch (identifierPolicy) {
				case ALREADY_ASSIGNED:
					Class<I> e = Reflections.propertyType(method);
					FluentMappingBuilder.this.identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(e);
					newColumn.primaryKey();
					break;
				default:
					throw new NotYetSupportedOperationException();
			}
			FluentMappingBuilder.this.identifierAccessor = propertyAccessor;
			// we could return null because the decorator return embedder.this for us, but I find cleaner to do so (if we change our mind)
			return FluentMappingBuilder.this;
		});
	}
	
	/**
	 * @return a new Column aded to the target table, throws an exception if already mapped
	 */
	private Column addMapping(String columnName, Class<?> columnType, PropertyAccessor<T, I> propertyAccessor) {
		Column newColumn = table.new Column(columnName, columnType);
		this.mapping.forEach(l -> {
			if (l.getFunction().equals(propertyAccessor)) {
				throw new IllegalArgumentException("Mapping is already defined by the method " + l.getFunction().getAccessor());
			}
			if (l.getColumn().equals(newColumn)) {
				throw new IllegalArgumentException("Mapping is already defined for " + columnName);
			}
		});
		this.mapping.add(new Linkage(propertyAccessor, newColumn));
		return newColumn;
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier> IFluentMappingBuilderOneToOneOptions<T, I> addOneToOne(Function<T, O> function,
																														Persister<O, J> persister) {
		CascadeOne<T, O, J> cascadeOne = new CascadeOne<>(function, persister);
		this.cascadeOnes.add(cascadeOne);
		// we declare the column on our side
		add(cascadeOne.targetProvider);
		IFluentMappingBuilderOneToOneOptions[] finalHack = new IFluentMappingBuilderOneToOneOptions[1];
		IFluentMappingBuilderOneToOneOptions<T, I> proxy = new Decorator<>(OneToOneOptions.class).decorate(this,
				(Class<IFluentMappingBuilderOneToOneOptions<T, I>>) (Class) IFluentMappingBuilderOneToOneOptions.class, new OneToOneOptions() {
					
					@Override
					public IFluentMappingBuilderOneToOneOptions cascade(CascadeType cascadeType, CascadeType... cascadeTypes) {
						cascadeOne.addCascadeType(cascadeType);
						for (CascadeType type : cascadeTypes) {
							cascadeOne.addCascadeType(type);
						}
						return finalHack[0];
					}
				});
		finalHack[0] = proxy;
		return proxy;
	}
	
	@Override
	public <O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> IFluentMappingBuilderOneToManyOptions<T, I, O> addOneToMany(
			Function<T, C> function, Persister<O, J> persister) {
		cascadeMany = new CascadeMany<>(function, persister);
		
		IFluentMappingBuilderOneToManyOptions[] finalHack = new IFluentMappingBuilderOneToManyOptions[1];
		IFluentMappingBuilderOneToManyOptions<T, I, O> proxy = new Decorator<>(OneToManyOptions.class).decorate(
				this,
				(Class<IFluentMappingBuilderOneToManyOptions<T, I, O>>) (Class) IFluentMappingBuilderOneToManyOptions.class,
				new OneToManyOptions() {
					@Override
					public IFluentMappingBuilderOneToManyOptions mappedBy(BiConsumer reverseLink) {
						cascadeMany.reverseMember = reverseLink;
						return finalHack[0];
					}
					
					@Override
					public IFluentMappingBuilderOneToManyOptions cascade(CascadeType cascadeType, CascadeType... cascadeTypes) {
						cascadeMany.addCascadeType(cascadeType);
						for (CascadeType type : cascadeTypes) {
							cascadeMany.addCascadeType(type);
						}
						return finalHack[0];
					}
				});
		finalHack[0] = proxy;
		return proxy;
	}
	
	@Override
	public IFluentMappingBuilder<T, I> embed(Function<T, ?> function) {
		insets.add(new Inset<>(function));
		return this;
	}
	
	private Map<PropertyAccessor, Column> collectMapping() {
		return mapping.stream().collect(HashMap::new, (hashMap, linkage) -> hashMap.put(linkage.getFunction(), linkage.getColumn()), (a, b) -> { });
	}
	
	@Override
	public ClassMappingStrategy<T, I> build(Dialect dialect) {
		assertColumnBindersRegistered(dialect);
		return buildClassMappingStrategy();
	}
	
	/**
	 * Asserts that binders of all mapped columns are present: it will throw en exception if the binder is not found
	 */
	private void assertColumnBindersRegistered(Dialect dialect) {
		mapping.stream().map(Linkage::getColumn).forEach(c -> dialect.getColumnBinderRegistry().getBinder(c));
	}
	
	@Override
	public Persister<T, I> build(PersistenceContext persistenceContext) {
		ClassMappingStrategy<T, I> mappingStrategy = build(persistenceContext.getDialect());
		Persister<T, I> localPersister = persistenceContext.add(mappingStrategy);
		if (!cascadeOnes.isEmpty()) {
			JoinedTablesPersister<T, I> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mappingStrategy);
			localPersister = joinedTablesPersister;
			// adding persistence flag setters on this side
			joinedTablesPersister.getPersisterListener().addInsertListener((IInsertListener<T>) SetPersistedFlagAfterInsertListener.INSTANCE);
			for (CascadeOne<T, ? extends Identified, ? extends StatefullIdentifier> cascadeOne : cascadeOnes) {
				Persister<Identified, StatefullIdentifier> targetPersister = (Persister<Identified, StatefullIdentifier>) cascadeOne.persister;
				
				// adding persistence flag setters on other side
				targetPersister.getPersisterListener().addInsertListener(SetPersistedFlagAfterInsertListener.INSTANCE);
				
				PropertyAccessor<Identified, Identified> propertyAccessor = PropertyAccessor.of(cascadeOne.member);
				// Finding joined columns:
				// - left one is given by current mapping strategy throught the property accessor.
				// - Right one is target primary key because we don't yet support "not owner of the property"
				Column leftColumn = mappingStrategy.getDefaultMappingStrategy().getPropertyToColumn().get(propertyAccessor);
				Column rightColumn = targetPersister.getTargetTable().getPrimaryKey();
				
				for (CascadeType cascadeType : cascadeOne.cascadeTypes) {
					switch (cascadeType) {
						case INSERT:
							localPersister.getPersisterListener().addInsertListener(new AfterInsertCascader<T, Identified>(targetPersister) {
								
								@Override
								protected void postTargetInsert(Iterable<Identified> iterables) {
									// Nothing to do. Identified#isPersisted flag should be fixed by target persister
								}
								
								@Override
								protected Identified getTarget(T o) {
									Identified target = cascadeOne.targetProvider.apply(o);
									// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
									if (target != null && !target.getId().isPersisted()) {
										return target;
									} else {
										return null;
									}
								}
							});
							break;
						case UPDATE:
							localPersister.getPersisterListener().addUpdateListener(new AfterUpdateCascader<T, Identified>(targetPersister) {
								
								@Override
								protected void postTargetUpdate(Iterable<Entry<Identified, Identified>> iterables) {
									// Nothing to do
								}
								
								@Override
								protected Entry<Identified, Identified> getTarget(T modifiedTrigger, T unmodifiedTrigger) {
									return new SimpleEntry<>(cascadeOne.targetProvider.apply(modifiedTrigger), cascadeOne.targetProvider.apply
											(unmodifiedTrigger));
								}
							});
							break;
						case DELETE:
							localPersister.getPersisterListener().addDeleteListener(new BeforeDeleteCascader<T, Identified>(targetPersister) {
								
								@Override
								protected void postTargetDelete(Iterable<Identified> iterables) {
								}
								
								@Override
								protected Identified getTarget(T o) {
									Identified target = cascadeOne.targetProvider.apply(o);
									// We only delete persisted instances (for logic and to prevent from non matching row count error)
									if (target != null && target.getId().isPersisted()) {
										return target;
									} else {
										return null;
									}
								}
							});
							// we add the delete roughly event since we suppose that if delete is required then there's no reason that roughly 
							// delete is not
							localPersister.getPersisterListener().addDeleteRoughlyListener(new BeforeDeleteRoughlyCascader<T, Identified>
									(targetPersister) {
								
								@Override
								protected void postTargetDelete(Iterable<Identified> iterables) {
								}
								
								@Override
								protected Identified getTarget(T o) {
									Identified target = cascadeOne.targetProvider.apply(o);
									// We only delete persisted instances (for logic and to prevent from non matching row count error)
									if (target != null && target.getId().isPersisted()) {
										return target;
									} else {
										return null;
									}
								}
							});
							break;
					}
				}
				
				IMutator targetSetter = propertyAccessor.getMutator();
				joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister,
						BeanRelationFixer.of(targetSetter::set),
						leftColumn, rightColumn, true);
			}
		}
		
		if (this.cascadeMany != null) {
			JoinedTablesPersister<T, I> joinedTablesPersister = new JoinedTablesPersister<>(persistenceContext, mappingStrategy);
			localPersister = joinedTablesPersister;
			
			Persister<Identified, StatefullIdentifier> targetPersister = (Persister<Identified, StatefullIdentifier>) this.cascadeMany.persister;
			
			// adding persistence flag setters on both side
			joinedTablesPersister.getPersisterListener().addInsertListener((IInsertListener<T>) SetPersistedFlagAfterInsertListener.INSTANCE);
			targetPersister.getPersisterListener().addInsertListener(SetPersistedFlagAfterInsertListener.INSTANCE);
			
			// finding joined columns: left one is primary key. Right one is given by the target strategy throught the property accessor
			Column leftColumn = localPersister.getTargetTable().getPrimaryKey();
			Function targetProvider = this.cascadeMany.targetProvider;
			if (cascadeMany.reverseMember == null) {
				throw new NotYetSupportedOperationException("Collection mapping without reverse property is not (yet) supported,"
						+ " please used \"mappedBy\" option do declare one for "
						+ Reflections.toString(new MethodReferenceCapturer<>(localPersister.getMappingStrategy().getClassToPersist()).capture(targetProvider)));
			}
			
			Class<? extends Identified> targetClass = cascadeMany.persister.getMappingStrategy().getClassToPersist();
			MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer<>(targetClass);
			Method reverseMember = methodReferenceCapturer.capture(cascadeMany.reverseMember);
			Column rightColumn = targetPersister.getMappingStrategy().getDefaultMappingStrategy().getPropertyToColumn().get(PropertyAccessor.of(reverseMember));
			if (rightColumn == null) {
				throw new NotYetSupportedOperationException("Reverse side mapping is not declared, please add the mapping of a "
						+ localPersister.getMappingStrategy().getClassToPersist().getSimpleName()
						+ " to persister of " + cascadeMany.persister.getMappingStrategy().getClassToPersist().getName());
			}
			
			for (CascadeType cascadeType : cascadeMany.cascadeTypes) {
				switch (cascadeType) {
					case INSERT:
						localPersister.getPersisterListener().addInsertListener(new AfterInsertCollectionCascader<T, Identified>(targetPersister) {
							
							@Override
							protected void postTargetInsert(Iterable<Identified> iterables) {
								// Nothing to do. Identified#isPersisted flag should be fixed by target persister
							}
							
							@Override
							protected Collection<Identified> getTargets(T o) {
								Collection<Identified> targets = (Collection<Identified>) targetProvider.apply(o);
								// We only insert non-persisted instances (for logic and to prevent duplicate primary key error)
								return Iterables.stream(targets)
										.filter(Objects::nonNull)
										.filter(t -> t == null || !t.getId().isPersisted())
										.collect(Collectors.toList());
							}
						});
						break;
					case UPDATE:
						localPersister.getPersisterListener().addUpdateListener(new AfterUpdateCollectionCascader<T, Identified>(targetPersister) {
							
							@Override
							public void afterUpdate(Iterable<Map.Entry<T, T>> iterables, boolean allColumnsStatement) {
								IdentifiedCollectionDiffer differ = new IdentifiedCollectionDiffer();
								iterables.forEach(entry -> {
									Set<Diff> diffSet = differ.diffSet(
											(Set) cascadeMany.targetProvider.apply(entry.getValue()),
											(Set) cascadeMany.targetProvider.apply(entry.getKey()));
									for (Diff diff : diffSet) {
										switch (diff.getState()) {
											case ADDED:
												targetPersister.insert(diff.getReplacingInstance());
												break;
											case HELD:
												// NB: update will only be done if necessary by target persister
												targetPersister.update(diff.getReplacingInstance(), diff.getSourceInstance(), allColumnsStatement);
												break;
											case REMOVED:
												targetPersister.delete(diff.getSourceInstance());
												break;
										}
									}
								});
							}
							
							@Override
							protected void postTargetUpdate(Iterable<Map.Entry<Identified, Identified>> iterables) {
								// Nothing to do
							}
							
							@Override
							protected Collection<Entry<Identified, Identified>> getTargets(T modifiedTrigger, T unmodifiedTrigger) {
								throw new NotYetSupportedOperationException();
							}
						});
						break;
					case DELETE:
						localPersister.getPersisterListener().addDeleteListener(new BeforeDeleteCollectionCascader<T, Identified>(targetPersister) {
							
							@Override
							protected void postTargetDelete(Iterable<Identified> iterables) {
							}
							
							@Override
							protected Collection<Identified> getTargets(T o) {
								Collection<Identified> targets = (Collection<Identified>) targetProvider.apply(o);
								// We only delete persisted instances (for logic and to prevent from non matching row count exception)
								return Iterables.stream(targets)
										.filter(Objects::nonNull)
										.filter(t -> t != null && t.getId().isPersisted())
										.collect(Collectors.toList());
							}
						});
						// we add the delete roughly event since we suppose that if delete is required then there's no reason that roughly delete is not
						localPersister.getPersisterListener().addDeleteRoughlyListener(new BeforeDeleteRoughlyCollectionCascader<T, Identified>(targetPersister) {
							@Override
							protected void postTargetDelete(Iterable<Identified> iterables) {
							}
							
							@Override
							protected Collection<Identified> getTargets(T o) {
								Collection<Identified> targets = (Collection<Identified>) targetProvider.apply(o);
								// We only delete persisted instances (for logic and to prevent from non matching row count exception)
								return Iterables.stream(targets)
										.filter(Objects::nonNull)
										.filter(t -> t != null && t.getId().isPersisted())
										.collect(Collectors.toList());
							}
						});
						break;
				}
			}
			
			IMutator targetSetter = PropertyAccessor.of(cascadeMany.member).getMutator();
			joinedTablesPersister.addPersister(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, targetPersister,
					BeanRelationFixer.of((BiConsumer) targetSetter::set, targetProvider, this.cascadeMany.collectionTargetClass, (BiConsumer) cascadeMany.reverseMember),
					leftColumn, rightColumn, true);
		}
		
		for (Inset embed : this.insets) {
			// Building the mapping of the value-object's fields to the table
			Map<String, Column> columnsPerName = localPersister.getTargetTable().mapColumnsOnName();
			Map<PropertyAccessor, Column> mapping = new HashMap<>();
			FieldIterator fieldIterator = new FieldIterator(embed.cascadingTargetClass);
			while (fieldIterator.hasNext()) {
				Field field = fieldIterator.next();
				Column column = columnsPerName.get(field.getName());
				if (column == null) {
					// Column isn't declared in table => we create one from field informations
					column = localPersister.getTargetTable().new Column(field.getName(), field.getType());
				}
				mapping.put(PropertyAccessor.of(field), column);
			}
			// We simply register a specialized mapping strategy for the field into the main strategy
			EmbeddedBeanMappingStrategy beanMappingStrategy = new EmbeddedBeanMappingStrategy(embed.cascadingTargetClass, mapping);
			mappingStrategy.put(PropertyAccessor.of(embed.member), beanMappingStrategy);
		}
		
		return localPersister;
	}

	private ClassMappingStrategy<T, I> buildClassMappingStrategy() {
		Map<PropertyAccessor, Column> columnMapping = collectMapping();
		List<Entry<PropertyAccessor, Column>> identifierProperties = columnMapping.entrySet().stream().filter(e -> e.getValue().isPrimaryKey())
				.collect(Collectors.toList());
		PropertyAccessor<T, I> identifierProperty;
		switch (identifierProperties.size()) {
			case 0:
				throw new IllegalArgumentException("Table without primary key is not supported");
			case 1:
				identifierProperty = (PropertyAccessor<T, I>) identifierProperties.get(0).getKey();
				break;
			default:
				throw new IllegalArgumentException("Multiple columned primary key is not supported");
		}
		
		return new ClassMappingStrategy<>(persistedClass, table, columnMapping, identifierProperty, this.identifierInsertionManager);
	}
	
	
	private class Linkage {
		
		private final PropertyAccessor function;
		private final Column column;
		private Class<Collection> targetManyType;
		
		private Linkage(PropertyAccessor function, Column column) {
			this.function = function;
			this.column = column;
		}
		
		public PropertyAccessor<T, ?> getFunction() {
			return function;
		}
		
		public Column getColumn() {
			return column;
		}
		
		public Class<Collection> getTargetManyType() {
			return targetManyType;
		}
		
		public void setTargetManyType(Class<Collection> targetManyType) {
			this.targetManyType = targetManyType;
		}
	}
	
	private class CascadeOne<SRC extends Identified, O extends Identified, J extends StatefullIdentifier> {
		
		private final Function<SRC, O> targetProvider;
		private final Persister<O, J> persister;
		private final Method member;
		private final Set<CascadeType> cascadeTypes = new HashSet<>();
		
		private CascadeOne(Function<SRC, O> targetProvider, Persister<O, J> persister) {
			this.targetProvider = targetProvider;
			this.persister = persister;
			// looking for the target type because its necessary to find its persister (and other objects). Done thru a method capturer (weird 
			// thing).
			this.member = captureLambdaMethod((Function) targetProvider);
		}
		
		public Persister<O, J> getPersister() {
			return persister;
		}
		
		public void addCascadeType(CascadeType cascadeType) {
			this.cascadeTypes.add(cascadeType);
		}
	}
	
	private class CascadeMany<SRC extends Identified, O extends Identified, J extends StatefullIdentifier, C extends Collection<O>> {
		
		private final Function<SRC, C> targetProvider;
		private final Persister<O, J> persister;
		private final Method member;
		private final Class<C> collectionTargetClass;
		private BiConsumer<O, SRC> reverseMember;
		private final Set<CascadeType> cascadeTypes = new HashSet<>();
		
		private CascadeMany(Function<SRC, C> targetProvider, Persister<O, J> persister) {
			this.targetProvider = targetProvider;
			this.persister = persister;
			// looking for the target type because its necessary to find its persister (and other objects). Done thru a method capturer (weird 
			// thing).
			this.member = captureLambdaMethod((Function) targetProvider);
			this.collectionTargetClass = (Class<C>) Reflections.onJavaBeanPropertyWrapper(member, member::getReturnType, () -> member
					.getParameterTypes()[0], null);
		}
		
		private CascadeMany(Function<SRC, C> targetProvider, Persister<O, J> persister, Class<C> collectionTargetClass) {
			this.targetProvider = targetProvider;
			this.persister = persister;
			// looking for the target type because its necesary to find its persister (and other objects). Done thru a method capturer (weird thing).
			this.member = captureLambdaMethod((Function) targetProvider);
			this.collectionTargetClass = collectionTargetClass;
		}
		
		public void addCascadeType(CascadeType cascadeType) {
			this.cascadeTypes.add(cascadeType);
		}
	}
	
	/**
	 * Represents a property that embeds a complex type
	 *
	 * @param <SRC> the owner type
	 * @param <TRGT> the target type
	 */
	private class Inset<SRC, TRGT> {
		private final Class<TRGT> cascadingTargetClass;
		private final Method member;
		
		private Inset(Function<SRC, TRGT> targetProvider) {
			// looking for the target type because its necesary to find its persister (and other objects). Done thru a method capturer (weird thing).
			this.member = captureLambdaMethod((Function) targetProvider);
			this.cascadingTargetClass = (Class<TRGT>) Reflections.onJavaBeanPropertyWrapper(member, member::getReturnType, () -> member
					.getParameterTypes()[0], null);
		}
	}
	
	private static class SetPersistedFlagAfterInsertListener extends NoopInsertListener<Identified> {
		
		public static final SetPersistedFlagAfterInsertListener INSTANCE = new SetPersistedFlagAfterInsertListener();
		
		@Override
		public void afterInsert(Iterable<Identified> iterables) {
			for (Identified t : iterables) {
				if (t.getId() instanceof PersistableIdentifier) {
					((PersistableIdentifier) t.getId()).setPersisted(true);
				}
			}
		}
	}
}

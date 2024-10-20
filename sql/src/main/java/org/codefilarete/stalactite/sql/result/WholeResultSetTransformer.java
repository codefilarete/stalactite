package org.codefilarete.stalactite.sql.result;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.stalactite.sql.result.ResultSetRowTransformer.BeanFactory;
import org.codefilarete.stalactite.sql.result.ResultSetRowTransformer.IdentifierArgBeanFactory;
import org.codefilarete.stalactite.sql.result.ResultSetRowTransformer.NoIdentifierBeanFactory;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.ThreadLocals;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;

import static org.codefilarete.stalactite.sql.result.WholeResultSetTransformer.AssemblyPolicy.ON_EACH_ROW;

/**
 * A class aimed at transforming a whole {@link ResultSet} into a graph of objects.
 * Graph creation is declared through {@link #add(String, ResultSetReader, Class, SerializableFunction, BeanRelationFixer)}, be aware that relations will be
 * unstacked/applied in order of declaration.
 * Instances of this class can be reused over multiple {@link ResultSet} (supposed to have same columns) and are thread-safe for iteration.
 * They can also be adapted to other {@link ResultSet}s that haven't the exact same column names by duplicating them with {@link #copyWithAliases(Function)}.
 * Moreover, they can also be cloned to another type of bean which uses the same column names with {@link #copyFor(Class, SerializableFunction)}.
 * 
 * @param <C> assembled bean type
 * @param <I> bean identifier type
 * @author Guillaume Mary
 * @see #add(String, ResultSetReader, BiConsumer)
 * @see #add(String, ResultSetReader, Class, SerializableFunction, BeanRelationFixer)
 * @see #transformAll(ResultSet) 
 */
public class WholeResultSetTransformer<C, I> implements ResultSetTransformer<C, I>, CopiableForAnotherQuery<C> {
	
	/**
	 * Per-Thread caches for created beans during {@link ResultSet} iteration.
	 * Created as a {@link ThreadLocal} to share it between all {@link WholeResultSetTransformer}s that are implied in the bean graph creation :
	 * first approach was to use an instance variable and initialize it on all instances before {@link ResultSet} iteration and release it after,
	 * but this design had several drawbacks:
	 * - non-thread-safe usage of instances (implying synchronization during whole iteration !)
	 * - instances piped with {@link #add(BeanRelationFixer, ResultSetRowTransformer)} were impossible to wire to the bean cache without cloning them to a
	 * {@link WholeResultSetTransformer}
	 */
	static final ThreadLocal<SimpleBeanCache> CURRENT_BEAN_CACHE = new ThreadLocal<>();
	
	static final ThreadLocal<Set<TreatedRelation>> CURRENT_TREATED_ASSEMBLERS = ThreadLocal.withInitial(HashSet::new);
	
	private final RootConverter<C, I> rootConverter;
	
	/** The list of relations that will assemble objects */
	private final KeepOrderSet<Assembler<C>> assemblers = new KeepOrderSet<>();
	
	/** Raw consumers, not linked to any beans, will be run on each row, if someone expects it to be run once per bean, then attach it to root converter */
	private final Set<ColumnConsumer<C, Object>> consumers = new HashSet<>();
	
	/**
	 * Constructor with root bean instantiation parameters
	 * 
	 * @param rootType main bean type
	 * @param beanKeyColumnName the name of the column that contains bean key
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean creator, bean key will be passed as argument. Not called if bean key is null (no instantiation needed)
	 */
	public WholeResultSetTransformer(Class<C> rootType, String beanKeyColumnName, ResultSetReader<I> reader, SerializableFunction<I, C> beanFactory) {
		this(new ResultSetRowTransformer<>(rootType, beanKeyColumnName, reader, beanFactory));
	}
	
	/**
	 * Constructor with root bean instantiation parameters as a default Java Bean constructor and setter for key value
	 *
	 * @param rootType main bean type
	 * @param beanKeyColumnName the name of the column that contains bean key
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean constructor. Not called if bean key is null (no instantiation needed)
	 * @param setter setter for bean key
	 */
	public WholeResultSetTransformer(Class<C> rootType, String beanKeyColumnName, ResultSetReader<I> reader, Supplier<C> beanFactory, BiConsumer<C, I> setter) {
		this(rootType, beanKeyColumnName, reader, i -> {
			C newInstance = beanFactory.get();
			setter.accept(newInstance, i);
			return newInstance;
		});
	}
	
	/**
	 * Constructor with root bean instantiation parameters.
	 * 
	 * With this constructor a root instance per {@link ResultSet} row will be created since there's no mean to
	 * distinguish a row instance from another because no key reader is given. This is expected to be used for flat data retrieval. 
	 *
	 * @param rootType main bean type
	 * @param beanFactory the bean creator
	 */
	public WholeResultSetTransformer(Class<C> rootType, SerializableSupplier<C> beanFactory) {
		this(new ResultSetRowTransformer<>(rootType, new NoIdentifierBeanFactory<>(beanFactory)));
	}
	
	/**
	 * Special constructor aimed at defining root transformer when other constructors are insufficient
	 * 
	 * @param rootTransformer transformer that will create graph root-beans from {@link ResultSet}
	 */
	public WholeResultSetTransformer(ResultSetRowTransformer<C, I> rootTransformer) {
		this.rootConverter = new CachingResultSetRowTransformer<>(rootTransformer);
	}
	
	/**
	 * Defines a complementary column that will be mapped on a bean property.
	 * Null values will be passed to the consumer, hence the property mapper must be "null-value proof".
	 * Those consumers will be run on each row, if this behavior is not expected then one can attach it to root converter. 
	 * 
	 * @param columnConsumer the object that will do the reading and mapping
	 */
	@Override
	public <O> WholeResultSetTransformer<C, I> add(ColumnConsumer<C, O> columnConsumer) {
		consumers.add((ColumnConsumer<C, Object>) columnConsumer);
		return this;
	}
	
	/**
	 * Adds a bean relation to the main/root object.
	 * 
	 * @param columnName the column name to read the bean key
	 * @param reader a reader for reading the bean key
	 * @param beanType the type of the new bean
	 * @param beanFactory a factory for creating the new bean, not called if bean key is null
	 * @param combiner a callback that will combine the newly created bean and the one of this transformer, called even with a null bean (v)
	 * @param <K> ResultSet column type
	 * @param <V> new bean type
	 * 
	 * @return this
	 * @see #add(String, ResultSetReader, Class, BeanRelationFixer) 
	 */
	public <K, V> WholeResultSetTransformer<C, I> add(String columnName, ResultSetReader<K> reader, Class<V> beanType,
													  SerializableFunction<K, V> beanFactory, BeanRelationFixer<C, V> combiner) {
		ResultSetRowTransformer<V, K> relatedBeanCreator = new ResultSetRowTransformer<>(beanType, columnName, reader, beanFactory);
		add(combiner, relatedBeanCreator);
		return this;
	}
	
	/**
	 * Adds a bean relation to the main/root object.
	 * It is a simplified version of {@link #add(String, ResultSetReader, Class, SerializableFunction, BeanRelationFixer)} where the factory is the
	 * default constructor of the given type.
	 * Be aware that the factory doesn't take the column (bean key) value as a parameter, if no default constructor exists please prefer
	 * {@link #add(String, ResultSetReader, Class, SerializableFunction, BeanRelationFixer)} or {@link #add(String, ResultSetReader, BiConsumer)}
	 * 
	 * @param columnName the column name to read the bean key
	 * @param reader a reader for reading the bean key
	 * @param beanType the type of the new bean
	 * @param combiner a callback that will combine the newly created bean and the one of this transformer, called even with a null bean (v)
	 * @param <K> ResultSet column type
	 * @param <V> new bean type
	 * 
	 * @return this
	 */
	public <K, V> WholeResultSetTransformer<C, I> add(String columnName, ResultSetReader<K> reader, Class<V> beanType, BeanRelationFixer<C, V> combiner) {
		add(columnName, reader, beanType, v -> Reflections.newInstance(beanType), combiner);
		return this;
	}
	
	/**
	 * Combines this transformer with another one through a bean relation, creating a bean graph.
	 * Be aware that a copy of relatedBeanCreator is made to make it uses cache of beans during {@link ResultSet} iteration.
	 * Hence, modifying relatedBeanCreator after won't affect this.
	 * 
	 * @param <K> the type of the other bean keys
	 * @param <V> the type of the other beans
	 * @param combiner the wire between instances of this transformer and those of the given one
	 * @param relatedBeanCreator the manager of the other beans
	 * @return this
	 */
	@Override
	public <K, V> WholeResultSetTransformer<C, I> add(BeanRelationFixer<C, V> combiner, ResultSetRowTransformer<V, K> relatedBeanCreator) {
		return add(combiner, relatedBeanCreator, ON_EACH_ROW);
	}
	
	/**
	 * Combines this transformer with another one through a bean relation, hence a bean graph is created.
	 * Be aware that a copy of relatedBeanCreator is made to make it uses cache of beans during {@link ResultSet} iteration.
	 * Hence, modifying relatedBeanCreator after won't affect this.
	 *
	 * @param <K> the type of the other bean keys
	 * @param <V> the type of the other beans
	 * @param combiner the wire between instances of this transformer and those of the given one
	 * @param relatedBeanCreator the manager of the other beans
	 * @return this
	 */
	public <K, V> WholeResultSetTransformer<C, I> add(BeanRelationFixer<C, V> combiner, ResultSetRowTransformer<V, K> relatedBeanCreator, AssemblyPolicy assemblyPolicy) {
		CachingResultSetRowTransformer<V, K> relatedBeanCreatorCopy = new CachingResultSetRowTransformer<>(relatedBeanCreator);
		return add(new Relation<>(combiner, relatedBeanCreatorCopy), assemblyPolicy);
	}
	
	/**
	 * Adds a very generic way to assemble {@link ResultSet} rows to a root bean.
	 * Be aware that any bean created by given assembler won't participate in current instance cache, if this is required then one should implement
	 * its own cache.
	 * Assembly will occur on each row ({@link ResultSetRowAssembler#assemble(Object, ResultSet)} will be call for each {@link ResultSet} row)
	 * 
	 * @param assembler a generic combiner of a root bean and each {@link ResultSet} row 
	 * @return this
	 * @see #add(ResultSetRowAssembler, AssemblyPolicy)
	 */
	public WholeResultSetTransformer<C, I> add(ResultSetRowAssembler<C> assembler) {
		return add(assembler, ON_EACH_ROW);
	}
	
	/**
	 * Adds a very generic way to assemble {@link ResultSet} rows to a root bean.
	 * Be aware that any bean created by given assembler won't participate in current instance cache, if this is required then one should implement
	 * its own cache.
	 *
	 * @param assembler a generic combiner of a root bean and each {@link ResultSet} row
	 * @param assemblyPolicy policy to decide if given assemble shall be invoked on each row or not
	 * @return this
	 */
	public WholeResultSetTransformer<C, I> add(ResultSetRowAssembler<C> assembler, AssemblyPolicy assemblyPolicy) {
		this.assemblers.add(new Assembler<>(assembler, assemblyPolicy));
		return this;
	}
	
	@Override
	public <T extends C> WholeResultSetTransformer<T, I> copyFor(Class<T> beanType, SerializableFunction<I, T> beanFactory) {
		ResultSetRowTransformer<T, I> newRootConverter = this.rootConverter.copyFor(beanType, beanFactory);
		return copy(newRootConverter);
	}
	
	@Override
	public <T extends C> WholeResultSetTransformer<T, I> copyFor(Class<T> beanType, SerializableSupplier<T> beanFactory) {
		ResultSetRowTransformer<T, I> newRootConverter = this.rootConverter.copyFor(beanType, beanFactory);
		return copy(newRootConverter);
	}
	
	private <T extends C> WholeResultSetTransformer<T, I> copy(ResultSetRowTransformer<T, I> newRootConverter) {
		// Making the copy
		WholeResultSetTransformer<T, I> result = new WholeResultSetTransformer<>(newRootConverter);
		// Note: combiners are shared, not copied 
		result.assemblers.addAll((Collection) this.assemblers);
		result.consumers.addAll((Collection) this.consumers);
		return result;
	}
	
	@Override
	public WholeResultSetTransformer<C, I> copyWithAliases(Function<String, String> columnMapping) {
		// NB: rootConverter can be cloned without a cache checking bean factory because it already has it due to previous assignments
		// (follow rootConverter assignments to be sure)
		ResultSetRowTransformer<C, I> rootConverterCopy = this.rootConverter.copyWithAliases(columnMapping);
		WholeResultSetTransformer<C, I> result = new WholeResultSetTransformer<>(rootConverterCopy);
		this.assemblers.forEach(assembler ->
				result.add(assembler.getResultSetRowAssembler().copyWithAliases(columnMapping), assembler.getPolicy())
		);
		this.consumers.forEach(assembler ->
				result.add(assembler.copyWithAliases(columnMapping))
		);
		return result;
	}
	
	@Override	// for adhoc return type
	public WholeResultSetTransformer<C, I> copyWithAliases(Map<String, String> columnMapping) {
		return copyWithAliases(columnMapping::get);
	}
	
	public Set<C> transformAll(ResultSet resultSet) {
		return transformAll(resultSet, Accumulators.toCollection(LinkedHashSet::new));
	}
	
	public <R, S> R transformAll(ResultSet resultSet, Accumulator<C, S, R> accumulator) {
		// We convert the ResultSet with an iteration over a ResultSetIterator that uses the transform(ResultSet) method
		ResultSetIterator<C> resultSetIterator = new ResultSetIterator<C>(resultSet) {
			@Override
			public C convert(ResultSet resultSet) throws SQLException {
				return transform(resultSet);
			}
		};
		return doWithBeanCache(() -> accumulator.collect(() -> resultSetIterator));
	}
	
	/**
	 * Executes given code by initializing internal caches so one can use {@link #transform(ResultSet)} without getting {@link NullPointerException}
	 * due to their absence. Hence, given code is expected to use {@link #transform(ResultSet)} else this method is useless.
	 * This method is not to be considered as a primary usage of this class since its purpose is to iterate over a whole {@link ResultSet}.
	 * 
	 * @param callable any code using {@link #transform(ResultSet)}
	 * @param <O> type of instance returned by this method which is the one returned by given code
	 * @return object returned by given {@link Supplier}
	 */
	public <O> O doWithBeanCache(Supplier<O> callable) {
		return ThreadLocals.doWithThreadLocal(CURRENT_BEAN_CACHE, SimpleBeanCache::new,
				(Supplier<O>) () -> ThreadLocals.doWithThreadLocal(CURRENT_TREATED_ASSEMBLERS, HashSet::new, callable));
	}
	
	/**
	 * <strong>This method is not expected to be called from outside but is public to respect interface implementation</strong>
	 * Note that it uses an internal {@link ThreadLocal} to ensure {@link AssemblyPolicy#ONCE_PER_BEAN}
	 * 
	 * @param resultSet not null
	 * @return current row root bean
	 * @throws SQLException if an error occurs while reading given {@link ResultSet}
	 */
	@Override
	public C transform(ResultSet resultSet) throws SQLException {
		// Can it be possible to have a null root bean ? if such we should add a if-null prevention. But what's the case ?
		C currentRowBean = rootConverter.transform(resultSet);
		// Not made with stream because it doesn't handle well checked Exception
		for (Assembler<C> entry : this.assemblers) {
			ResultSetRowAssembler<C> rowAssembler = entry.getResultSetRowAssembler();
			switch (entry.getPolicy()) {
				case ON_EACH_ROW:
					rowAssembler.assemble(currentRowBean, resultSet);
					break;
				case ONCE_PER_BEAN:
					// we check if relation has already been treated for current bean : the key to be checked is a TreatedRelation
					TreatedRelation<C> treatedRelation = new TreatedRelation<>(currentRowBean, rowAssembler);
					Set<TreatedRelation> treatedRelations = CURRENT_TREATED_ASSEMBLERS.get();
					if (!treatedRelations.contains(treatedRelation)) {
						rowAssembler.assemble(currentRowBean, resultSet);
						treatedRelations.add(treatedRelation);
					}
					break;
			}
		}
		this.consumers.forEach(c -> c.assemble(currentRowBean, resultSet));
		return currentRowBean;
	}
	
	/**
	 * A relation between a property mutator (setter) and the provider of the bean to be given as the setter argument
	 * 
	 * @param <K>
	 * @param <V>
	 */
	private static class Relation<K, V> implements ResultSetRowAssembler<K> {
		
		private final BeanRelationFixer<K, V> relationFixer;
		
		private final CachingResultSetRowTransformer<V, ?> transformer;
		
		public Relation(BeanRelationFixer<K, V> relationFixer, CachingResultSetRowTransformer<V, ?> transformer) {
			this.relationFixer = relationFixer;
			this.transformer = transformer;
		}
		
		@Override
		public void assemble(@Nonnull K bean, ResultSet resultSet) {
			// getting the bean
			V value = transformer.transform(resultSet);
			// applying it to the setter
			if (value != null) {
				relationFixer.apply(bean, value);
			}
		}

		@Override
		public Relation<K, V> copyWithAliases(Function<String, String> columnMapping) {
			return new Relation<>(relationFixer, new CachingResultSetRowTransformer<>(transformer.transformer.copyWithAliases(columnMapping)));
		}
	}
	
	private static class Assembler<O> {
		
		private final ResultSetRowAssembler<O> resultSetRowAssembler;
		
		private final AssemblyPolicy policy;
		
		private Assembler(ResultSetRowAssembler<O> resultSetRowAssembler, AssemblyPolicy policy) {
			this.resultSetRowAssembler = resultSetRowAssembler;
			this.policy = policy;
		}
		
		public ResultSetRowAssembler<O> getResultSetRowAssembler() {
			return resultSetRowAssembler;
		}
		
		public AssemblyPolicy getPolicy() {
			return policy;
		}
		
		/**
		 * Implementation to avoid collision in Set, based on {@link ResultSetRowAssembler} only because we don't want to assemble beans twice
		 * because their relation differ in {@link AssemblyPolicy}, there one can't specify twice same combiner with different policies.
		 */
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			
			Assembler assembler = (Assembler) o;
			return resultSetRowAssembler.equals(assembler.resultSetRowAssembler);
		}
		
		/**
		 * Implementation to avoid collision in Set, based on {@link ResultSetRowAssembler} only because we don't want to assemble beans twice
		 * because their relation differ in {@link AssemblyPolicy}, there one can't specify twice same combiner with different policies.
		 */
		@Override
		public int hashCode() {
			return resultSetRowAssembler.hashCode();
		}
	}
	
	/**
	 * A small class that stores a relation between a root bean ad an assembler. It acts as a key in {@link HashSet} to mark a relation as treated
	 * to prevent the relation to be applied multiple times whereas it shouldn't, according to the {@link AssemblyPolicy#ONCE_PER_BEAN} strategy.
	 * 
	 * @param <K> root bean type
	 */
	private static class TreatedRelation<K> {
		
		private final K rootBean;
		
		private final ResultSetRowAssembler<K> assembler;
		
		private TreatedRelation(K rootBean, ResultSetRowAssembler<K> assembler) {
			this.rootBean = rootBean;
			this.assembler = assembler;
		}
		
		/** Implementation to avoid collision in Set */
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			
			TreatedRelation<?> that = (TreatedRelation<?>) o;
			
			if (!rootBean.equals(that.rootBean)) return false;
			return assembler.equals(that.assembler);
		}
		
		/** Implementation to avoid collision in Set */
		@Override
		public int hashCode() {
			int result = rootBean.hashCode();
			result = 31 * result + assembler.hashCode();
			return result;
		}
	}
	
	/**
	 * Policy introduced to specify if assembly shall be done for each row of a {@link ResultSet} (case of {@link java.util.Collection}
	 * to be filled for instance) or only once per root bean (simple setter case)
	 */
	public enum AssemblyPolicy {
		/** Specifies that assembly shall be done on each row of a {@link ResultSet} ({@link java.util.Collection} case) */
		ON_EACH_ROW,
		/** Specifies that assembly shall be done once (and only onnce) per root bean (setter case) */
		ONCE_PER_BEAN
	}
	
	private interface RootConverter<C, I> {
		
		<T extends C> ResultSetRowTransformer<T, I> copyFor(Class<T> beanType, SerializableFunction<I, T> beanFactory);
		
		<T extends C> ResultSetRowTransformer<T, I> copyFor(Class<T> beanType, SerializableSupplier<T> beanFactory);
		
		ResultSetRowTransformer<C, I> copyWithAliases(Function<String, String> columnMapping);
		
		C transform(ResultSet resultSet);
	}
	
	/**
	 * Cache system over a {@link ResultSetRowTransformer} that gives already created instance if bean key matches.
	 * Due to being based on bean key, this cache doesn't make sense with beans that don't have key (no-arg constructor),
	 * thus, is such case, a new instance will be created each time {@link #transform(ResultSet)} is called.
	 */
	private static class CachingResultSetRowTransformer<C, I> implements RootConverter<C, I> {
		
		private final ResultSetRowTransformer<C, I> transformer;
		
		private final Function<ResultSet, C> newInstanceProvider;
		
		private CachingResultSetRowTransformer(ResultSetRowTransformer<C, I> transformer) {
			this.transformer = transformer;
			// computing the instance creator according to the given bean factory
			BeanFactory<C> beanFactory = transformer.getBeanFactory();
			if (beanFactory instanceof ResultSetRowTransformer.NoIdentifierBeanFactory) {
				newInstanceProvider = rs -> ((NoIdentifierBeanFactory<C>) beanFactory).createInstance();
			} else if (beanFactory instanceof ResultSetRowTransformer.IdentifierArgBeanFactory) {
				newInstanceProvider = rs -> {
					I beanKey = ((IdentifierArgBeanFactory<I, C>) beanFactory).readBeanKey(rs);
					try {
						// we don't call the cache if the bean key is null
						return beanKey == null ? null : CURRENT_BEAN_CACHE.get().computeIfAbsent(transformer.getBeanType(), beanKey, i -> transformer.transform(rs));
					} catch (ClassCastException cce) {
						// Trying to give a more accurate reason that the default one
						// Put into places for rare cases of misusage by wrapping code that loses reader and bean factory types,
						// ending with a constructor input type error
						MethodReferenceCapturer methodReferenceCapturer = new MethodReferenceCapturer();
						throw new ClassCastException("Can't apply " + beanKey + " on constructor "
								+ Reflections.toString(methodReferenceCapturer.findExecutable(((IdentifierArgBeanFactory<I, C>) beanFactory).getFactory())) + " : " + cce.getMessage());
					}
				};
			} else {
				// should not happen
				throw new IllegalArgumentException("Class " + Reflections.toString(beanFactory.getClass()) + " is not implemented");
			}
		}
		
		public C transform(ResultSet resultSet) {
			return newInstanceProvider.apply(resultSet);
		}
		
		@Override
		public <T extends C> ResultSetRowTransformer<T, I> copyFor(Class<T> beanType, SerializableFunction<I, T> beanFactory) {
			return this.transformer.copyFor(beanType, beanFactory);
		}
		
		@Override
		public <T extends C> ResultSetRowTransformer<T, I> copyFor(Class<T> beanType, SerializableSupplier<T> beanFactory) {
			return this.transformer.copyFor(beanType, beanFactory);
		}
		
		@Override
		public ResultSetRowTransformer<C, I> copyWithAliases(Function<String, String> columnMapping) {
			return this.transformer.copyWithAliases(columnMapping);
		}
	}
}

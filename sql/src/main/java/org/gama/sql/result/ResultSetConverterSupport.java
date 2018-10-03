package org.gama.sql.result;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.gama.lang.Reflections;
import org.gama.lang.ThreadLocals;
import org.gama.lang.bean.Converter;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.ThrowingSupplier;
import org.gama.sql.binder.ResultSetReader;

/**
 * A class aimed at transforming a whole {@link ResultSet} into a graph of objects.
 * Graph creation is declared through {@link #add(String, ResultSetReader, Class, Function, BiConsumer)}, be aware that relations will be
 * unstacked/applied in order of declaration.
 * Instances of this class can be reused over multiple {@link ResultSet} (supposed to have same columns) and are thread-safe for iteration.
 * They can also be adapt to other {@link ResultSet}s that haven't the exact same column names by duplicating them with {@link #copyWithAliases(Function)}.
 * Moreover they can also be cloned to another type of bean which uses the same column names with {@link #copyFor(Class, Function)}.
 * 
 * @param <C> assembled bean type
 * @param <I> bean identifier type
 * @author Guillaume Mary
 * @see #add(String, ResultSetReader, BiConsumer)
 * @see #add(String, ResultSetReader, Class, Function, BiConsumer)
 */
public class ResultSetConverterSupport<I, C> implements ResultSetConverter<I, C>, Converter<ResultSet, List<C>, SQLException> {
	
	/**
	 * Cache for created beans during {@link ResultSet} iteration.
	 * Created as a ThreadLocal to share it between all {@link ResultSetConverterSupport}s that are implied in the bean graph creation : first approach
	 * was to use an instance variable and initialize it on all instances before {@link ResultSet} iteration and release it after, but this design
	 * had several drawbacks:
	 * - non-thread-safe usage of instances (implying synchronization during whole iteration !)
	 * - instances piped with {@link #add(ResultSetRowConverter, BiConsumer)} were impossible to wire to the bean cache without cloning them to a
	 * {@link ResultSetConverterSupport}
	 * 
	 */
	static final ThreadLocal<SimpleBeanCache> BEAN_CACHE = new ThreadLocal<>();
	
	private final ResultSetRowConverter<I, C> rootConverter;
	
	/**
	 * Bean factory given at instanciation time, this is the "real one", the other in the rootConverter points to it but is decorated with a bean
	 * cache checking through {@link #computeInstanceIfCacheMiss(Class, Object, ThrowingSupplier)}
	 */
	private final Function<I, C> beanFactory;
	
	/** The list of relations that will assemble objects */
	private final List<ResultSetRowAssembler<C>> combiners = new ArrayList<>();
	
	/**
	 * Constructor with root bean instanciation parameters
	 * 
	 * @param rootType main bean type
	 * @param columnName the name of the column that contains bean key
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean creator, bean key will be passed as argument. Not called if bean key is null (no instanciation needed)
	 */
	public ResultSetConverterSupport(Class<C> rootType, String columnName, ResultSetReader<I> reader, Function<I, C> beanFactory) {
		this.rootConverter = new ResultSetRowConverter<>(rootType, columnName, reader,
				k -> computeInstanceIfCacheMiss(rootType, k, () -> beanFactory.apply(k)));
		this.beanFactory = beanFactory;
	}
	
	/**
	 * Constructor with root bean instanciation parameters as a default Java Bean constructor and setter for key value
	 *
	 * @param rootType main bean type
	 * @param columnName the name of the column that contains bean key
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean constructor. Not called if bean key is null (no instanciation needed)
	 * @param setter setter for bean key
	 */
	public ResultSetConverterSupport(Class<C> rootType, String columnName, ResultSetReader<I> reader, Supplier<C> beanFactory, BiConsumer<C, I> setter) {
		this(rootType, columnName, reader, i -> {
			C newInstance = beanFactory.get();
			setter.accept(newInstance, i);
			return newInstance;
		});
	}
	
	/**
	 * Special constructor made for cloning, no reason to expose it outside
	 * @param rootConverter
	 * @param factory
	 */
	public ResultSetConverterSupport(ResultSetRowConverter<I, C> rootConverter, Function<I, C> factory) {
		this.rootConverter = rootConverter;
		this.beanFactory = factory;
	}
	
	/**
	 * Defines a complementary column that will be mapped on a bean property.
	 * Null values will be passed to the consumer, hence the property mapper must be "null-value proof".
	 * 
	 * @param columnConsumer the object that will do the reading and mapping
	 */
	@Override
	public void add(ColumnConsumer<C, ?> columnConsumer) {
		rootConverter.add(columnConsumer);
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
	 * @see #add(String, ResultSetReader, Class, BiConsumer) 
	 */
	public <K, V> ResultSetConverterSupport<I, C> add(String columnName, ResultSetReader<K> reader, Class<V> beanType, Function<K, V> beanFactory, BiConsumer<C, V> combiner) {
		ResultSetConverterSupport<K, V> relatedBeanCreator = new ResultSetConverterSupport<>(beanType, columnName, reader, beanFactory);
		add(relatedBeanCreator, combiner);
		return this;
	}
	
	/**
	 * Adds a bean relation to the main/root object.
	 * It is a simplified version of {@link #add(String, ResultSetReader, Class, Function, BiConsumer)} where the factory is the default constructor
	 * of the given type.
	 * Be aware that the factory doesn't take the column (bean key) value as a parameter, if no default constructor exists please prefer
	 * {@link #add(String, ResultSetReader, Class, Function, BiConsumer)} or {@link #add(String, ResultSetReader, BiConsumer)}
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
	public <K, V> ResultSetConverterSupport<I, C> add(String columnName, ResultSetReader<K> reader, Class<V> beanType, BiConsumer<C, V> combiner) {
		add(columnName, reader, beanType, v -> Reflections.newInstance(beanType), combiner);
		return this;
	}
	
	/**
	 * Combines this transformer with another one through a bean relation, hence a bean graph is created.
	 * Be aware that modifying relatedBeanCreator after will also affect this.
	 * 
	 * @param relatedBeanCreator the manager of the other beans
	 * @param combiner the wire between instances of this transformer and those of the given one
	 * @param <K> the type of the other bean keys
	 * @param <V> the type of the other beans
	 * @return this
	 */
	public <K, V> ResultSetConverterSupport<I, C> add(ResultSetConverterSupport<K, V> relatedBeanCreator, BiConsumer<C, V> combiner) {
		combiners.add(new Relation<>(combiner, relatedBeanCreator));
		return this;
	}
	
	/**
	 * Same as {@link #add(ResultSetConverterSupport, BiConsumer)} but for {@link ResultSetRowConverter}.
	 * Be aware that a copy of relatedBeanCreator is made to make it use cache of beans during {@link ResultSet} iteration. This is different from
	 * the other add(..) method that doesn't needs cloning of the relatedBeanCreator.
	 * Hence, modifying relatedBeanCreator after won't affect this.
	 * 
	 * @param relatedBeanCreator the manager of the other beans
	 * @param combiner the wire between instances of this transformer and those of the given one
	 * @param <K> the type of the other bean keys
	 * @param <V> the type of the other beans
	 * @return this
	 */
	public <K, V> ResultSetConverterSupport<I, C> add(ResultSetRowConverter<K, V> relatedBeanCreator, BiConsumer<C, V> combiner) {
		Class<V> beanType = relatedBeanCreator.getBeanType();
		Function<K, V> relatedBeanFactory = relatedBeanCreator.getBeanFactory();
		// ResultSetRowConverter doesn't have a cache system, so we decorate its factory with a cache checking
		ResultSetRowConverter<K, V> relatedBeanCreatorCopy = relatedBeanCreator.copyFor(beanType,
				beanKey -> computeInstanceIfCacheMiss(beanType, beanKey, () -> relatedBeanFactory.apply(beanKey))
		);
		relatedBeanCreatorCopy.getConsumers().addAll(relatedBeanCreator.getConsumers());
		this.combiners.add(new Relation<>(combiner, relatedBeanCreatorCopy));
		return this;
	}
	
	/**
	 * Adds a very generic way to assemble {@link ResultSet} rows to a root bean.
	 * ARGUMENT IS NOT TOOK INTO ACCOUNT BY {@link #copyFor(Class, Function)} neither {@link #copyWithAliases(Function)} nor {@link #copyWithAliases(Map)}
	 * 
	 * @param assembler a generic combiner of a root bean and each {@link ResultSet} row 
	 * @return this
	 */
	public ResultSetConverterSupport<I, C> add(ResultSetRowAssembler<C> assembler) {
		this.combiners.add(assembler);
		return this;
	}
	
	@Override
	public <T extends C> ResultSetConverterSupport<I, T> copyFor(Class<T> beanType, Function<I, T> beanFactory) {
		// ResultSetRowConverter doesn't have a cache system, so we decorate its factory with a cache checking
		ResultSetRowConverter<I, T> rootConverterCopy = this.rootConverter.copyFor(beanType,
				beanKey -> computeInstanceIfCacheMiss(beanType, beanKey, () -> beanFactory.apply(beanKey))
		);
		// Making the copy
		ResultSetConverterSupport<I, T> result = new ResultSetConverterSupport<>(rootConverterCopy, beanFactory);
		this.combiners.forEach(c -> {
			if (c instanceof Relation) {
				result.combiners.add(new Relation<>(((Relation) c).relationFixer, ((Relation) c).transformer));
			}
		});
		return result;
	}
	
	@Override
	public ResultSetConverterSupport<I, C> copyWithAliases(Function<String, String> columnMapping) {
		// NB: rootConverter can be cloned without a cache checking bean factory because it already has it due to previous assignements
		// (follow rootConverter assignements to be sure)
		ResultSetRowConverter<I, C> rootConverterCopy = this.rootConverter.copyWithAliases(columnMapping);
		this.rootConverter.getBeanFactory();
		ResultSetConverterSupport<I, C> result = new ResultSetConverterSupport<>(rootConverterCopy, this.beanFactory);
		this.combiners.forEach(c -> {
			if (c instanceof Relation) {
				result.combiners.add(new Relation<>(((Relation) c).relationFixer, ((Relation) c).transformer.copyWithAliases(columnMapping)));
			}
		});
		return result;
	}
	
	@Override	// for adhoc return type
	public ResultSetConverterSupport<I, C> copyWithAliases(Map<String, String> columnMapping) {
		return copyWithAliases(columnMapping::get);
	}
	
	@Override
	public List<C> convert(ResultSet resultSet) {
		// We convert the ResultSet with an iteration over a ResultSetIterator that uses the transform(ResultSet) method
		ResultSetIterator<C> resultSetIterator = new ResultSetIterator<C>(resultSet) {
			@Override
			public C convert(ResultSet resultSet) throws SQLException {
				return ResultSetConverterSupport.this.transform(resultSet);
			}
		};
		return doWithBeanCache(() -> Iterables.stream(resultSetIterator).collect(Collectors.toList()));
	}
	
	public <O> O doWithBeanCache(Supplier<O> callable) {
		return ThreadLocals.doWithThreadLocal(BEAN_CACHE, SimpleBeanCache::new, callable);
	}
	
	@Override
	public C transform(ResultSet resultSet) throws SQLException {
		// Can it be possible to have a null root bean ? if such we should add a if-null prevention. But what's the case ?
		C currentRowBean = rootConverter.convert(resultSet);
		for (ResultSetRowAssembler<C> combiner : this.combiners) {
			combiner.assemble(currentRowBean, resultSet);
		}
		return currentRowBean;
	}
	
	/**
	 * Made to expose bean cache checking 
	 */
	private static <C, K> C computeInstanceIfCacheMiss(Class<C> instanceType, K beanKey, ThrowingSupplier<Object, RuntimeException> factory) {
		// we don't call the cache if the bean key is null
		return beanKey == null ? null : BEAN_CACHE.get().computeIfAbsent(instanceType, beanKey, factory);
	}
	
	/**
	 * A relation between a property mutator (setter) and the provider of the bean to be given as the setter argument
	 * 
	 * @param <K>
	 * @param <V>
	 */
	public static class Relation<K, V> implements ResultSetRowAssembler<K> {
		
		private final BiConsumer<K, V> relationFixer;
		
		private final ResultSetConverter<?, V> transformer;
		
		public Relation(BiConsumer<K, V> relationFixer, ResultSetConverter<?, V> transformer) {
			this.relationFixer = relationFixer;
			this.transformer = transformer;
		}
		
		@Override
		public void assemble(@Nonnull K bean, ResultSet resultSet) throws SQLException {
			// getting the bean
			V value = transformer.transform(resultSet);
			// applying it to the setter
			relationFixer.accept(bean, value);
		}
	}
}
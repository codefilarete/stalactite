package org.codefilarete.stalactite.sql.result;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;
import org.codefilarete.tool.bean.Factory;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * A class aimed at creating flat (no graph) beans from a {@link ResultSet} row.
 * Will read a main/root column that determines if a bean can be created from it (is null ?), then applies some more "bean filler" that are
 * defined with {@link ColumnConsumer}.
 * Instances of this class can be reused over multiple {@link ResultSet} (supposed to have same columns).
 * They can also be adapted to other {@link ResultSet}s that haven't the exact same column names by duplicating them with {@link #copyWithAliases(Function)}.
 * Moreover they can also be cloned to another type of bean which uses the same column names with {@link #copyFor(Class, SerializableFunction)}.
 * 
 * @param <I> the type of the bean key (Input)
 * @param <C> the type of the bean
 *     
 * @author Guillaume Mary
 */
public class ResultSetRowTransformer<I, C> implements ResultSetTransformer<I, C>, ResultSetRowAssembler<C> {
	
	private final BeanFactory<C> beanFactory;
	
	private final Class<C> beanType;
	
	private final Set<ColumnConsumer<C, Object>> consumers = new HashSet<>();
	
	private final Map<BeanRelationFixer<C, Object>, ResultSetRowTransformer<Object, Object>> relations = new HashMap<>();
	
	/**
	 * Constructor focused on simple cases where beans are built only from one column key.
	 * Prefer {@link #ResultSetRowTransformer(Class, ColumnReader, SerializableFunction)} for more general purpose cases (multiple columns key)
	 * 
	 * @param columnName the name of the column that contains bean key
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean creator, bean key will be passed as argument. Not called if bean key is null (no instanciation needed)
	 */
	public ResultSetRowTransformer(Class<C> beanType, String columnName, ResultSetReader<I> reader, SerializableFunction<I, C> beanFactory) {
		this(beanType, new IdentifierArgBeanFactory<>(new SingleColumnReader<>(columnName, reader), beanFactory));
	}
	
	/**
	 * Constructor with main and mandatory arguments
	 *
	 * @param beanType type of built instances
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean creator, bean key will be passed as argument. Not called if bean key is null (no instanciation needed)
	 */
	public ResultSetRowTransformer(Class<C> beanType, ColumnReader<I> reader, SerializableFunction<I, C> beanFactory) {
		this(beanType, new IdentifierArgBeanFactory<>(reader, beanFactory));
	}
	
	/**
	 * Constructor with main and mandatory arguments.
	 * With  this constructor an instance per {@link ResultSet} row will be created since there's no mean to distinguish a row instance from another
	 * because no key reader is given
	 *
	 * @param beanType type of built instances
	 * @param beanFactory the bean creator
	 */
	public ResultSetRowTransformer(Class<C> beanType, SerializableSupplier<C> beanFactory) {
		this(beanType, new NoIdentifierBeanFactory<>(beanFactory));
	}
	
	/**
	 * For internal usage
	 * @param beanType type of built instances
	 * @param beanFactory bean factory to use as contructor
	 */
	ResultSetRowTransformer(Class<C> beanType, BeanFactory<C> beanFactory) {
		this.beanType = beanType;
		this.beanFactory = beanFactory;
	}
	
	public Class<C> getBeanType() {
		return beanType;
	}
	
	public BeanFactory<C> getBeanFactory() {
		return beanFactory;
	}
	
	/**
	 * Gives {@link ColumnConsumer}s of this instances
	 * 
	 * @return not null
	 */
	public Set<ColumnConsumer<C, Object>> getConsumers() {
		return consumers;
	}
	
	public Map<BeanRelationFixer<C, Object>, ResultSetRowTransformer<Object, Object>> getRelations() {
		return relations;
	}
	
	/**
	 * Defines a complementary column that will be mapped on a bean property.
	 * Null values will be passed to the consumer, hence the property mapper must be "null-value proof".
	 * 
	 * @param columnConsumer the object that will do reading and mapping
	 * @return this
	 */
	@Override
	public <O> ResultSetRowTransformer<I, C> add(ColumnConsumer<C, O> columnConsumer) {
		this.consumers.add((ColumnConsumer<C, Object>) columnConsumer);
		return this;
	}
	
	@Override
	public <K, V> ResultSetRowTransformer<I, C> add(BeanRelationFixer<C, V> combiner, ResultSetRowTransformer<K, V> relatedBeanCreator) {
		this.relations.put((BeanRelationFixer) combiner, (ResultSetRowTransformer) relatedBeanCreator);
		return this;
	}
	
	@Override
	public <T extends C> ResultSetRowTransformer<I, T> copyFor(Class<T> beanType, SerializableFunction<I, T> beanFactory) {
		if (!(this.beanFactory instanceof IdentifierArgBeanFactory)) {
			throw new UnsupportedOperationException("This instance can only be cloned with an identifier-arg constructor because it was created with one");
		}
		ResultSetRowTransformer<I, T> result = new ResultSetRowTransformer<>(beanType, ((IdentifierArgBeanFactory) this.beanFactory).copyFor(beanFactory));
		result.consumers.addAll((Set) this.consumers);
		this.relations.forEach((consumer, transformer) -> {
			result.relations.put((BeanRelationFixer) consumer, transformer.copyFor(transformer.beanType, ((SerializableFunction) transformer.getBeanFactory().getFactory())));
		});
		return result;
	}
	
	@Override
	public <T extends C> ResultSetRowTransformer<I, T> copyFor(Class<T> beanType, SerializableSupplier<T> beanFactory) {
		if (!(this.beanFactory instanceof NoIdentifierBeanFactory)) {
			throw new UnsupportedOperationException("This instance can only be cloned with a no-arg constructor because it was created with one");
		}
		ResultSetRowTransformer<I, T> result = new ResultSetRowTransformer<>(beanType, ((NoIdentifierBeanFactory) this.beanFactory).copyFor(beanFactory));
		result.consumers.addAll((Set) this.consumers);
		this.relations.forEach((consumer, transformer) -> {
				result.relations.put((BeanRelationFixer) consumer, transformer.copyFor(transformer.beanType, ((SerializableSupplier) transformer.getBeanFactory().getFactory())));
		});
		return result;
	}
	
	@Override
	public ResultSetRowTransformer<I, C> copyWithAliases(Function<String, String> columnMapping) {
		ResultSetRowTransformer<I, C> result = new ResultSetRowTransformer<>(this.beanType, this.beanFactory.copyWithAliases(columnMapping));
		this.consumers.forEach(c -> result.add(c.copyWithAliases(columnMapping)));
		this.relations.forEach((consumer, transformer) -> result.add(consumer, transformer.copyWithAliases(columnMapping)));
		return result;
	}
	
	@Override	// for adhoc return type
	public ResultSetRowTransformer<I, C> copyWithAliases(Map<String, String> columnMapping) {
		// equivalent to super.copyWithAlias(..) but because inteface default method can't be invoked we have to copy/paste its code ;(
		return copyWithAliases(columnMapping::get);
	}
	
	/**
	 * Converts the current {@link ResultSet} row into a bean.
	 * Depending on implementation of factory, it may return a brand new instance or a cached one (if the bean key is already known for instance).
	 * Consumers will be applied to instance returned by the factory, as a consequence if bean comes from a cache it will be completed again, this may
	 * do some extra work in case of simple property.
	 *
	 * @param resultSet not null
	 * @return an instance of T, newly created or not according to implementation
	 */
	@Override
	public C transform(ResultSet resultSet) {
		return nullable(this.beanFactory.createInstance(resultSet))
				.invoke(b -> assemble(b, resultSet))
				.get();
	}
	
	/**
	 * Implementation that applies all {@link ColumnConsumer} to the given {@link ResultSet}
	 *
	 * @param rootBean the bean built for the row
	 * @param input any {@link ResultSet} positioned at row that must be read
	 */
	@Override
	public void assemble(C rootBean, ResultSet input) {
		// we consume simple properties
		for (ColumnConsumer<C, ?> consumer : consumers) {
			consumer.assemble(rootBean, input);
		}
		
		// we set related beans
		for (Entry<BeanRelationFixer<C, Object>, ResultSetRowTransformer<Object, Object>> entry : relations.entrySet()) {
			Object relatedBean = entry.getValue().transform(input);
			if (relatedBean != null) {	// null related bean is considered as no relation
				entry.getKey().apply(rootBean, relatedBean);
			}
		}
	}
	
	/**
	 * Definition of a factory focused on {@link ResultSetRowTransformer} usage
	 * @param <C> created type
	 */
	public interface BeanFactory<C> extends Factory<ResultSet, C>, CopiableForAnotherQuery<C> {
		
		Serializable getFactory();
		
		@Override
		BeanFactory<C> copyWithAliases(Function<String, String> columnMapping);
	}
	
	/**
	 * Specialization of {@link BeanFactory} for no-arg contructor classes.
	 * 
	 * @param <C> created type
	 */
	public static class NoIdentifierBeanFactory<C> implements BeanFactory<C> {
		
		private final SerializableSupplier<C> factory;
		
		public NoIdentifierBeanFactory(SerializableSupplier<C> factory) {
			this.factory = factory;
		}
		
		public C createInstance() {
			return factory.get();
		}
		
		public SerializableSupplier<C> getFactory() {
			return factory;
		}
		
		@Override
		public C createInstance(ResultSet input) {
			return factory.get();
		}
		
		@Override
		public BeanFactory<C> copyWithAliases(Function<String, String> columnMapping) {
			return new NoIdentifierBeanFactory<>(factory);
		}
		
		/**
		 * Used to copy this instance for a subclass of its bean type, required for inheritance implementation.
		 * @param beanFactory subclass no-arg constructor
		 * @param <T> subclass type
		 * @return a new {@link BeanFactory} for a subclass bean which read same column as this instance
		 */
		public <T extends C> NoIdentifierBeanFactory<T> copyFor(SerializableSupplier<T> beanFactory) {
			return new NoIdentifierBeanFactory<>(beanFactory);
		}
	}
	
	/**
	 * Specialization of {@link BeanFactory} for one-arg constructor classes
	 * 
	 * @param <I> constructor input type (also column type)
	 * @param <C> bean type
	 */
	public static class IdentifierArgBeanFactory<I, C> implements BeanFactory<C> {
		
		/** {@link ResultSet} reader */
		private final ColumnReader<I> reader;
		
		/** one-arg bean constructor */
		private final SerializableFunction<I, C> factory;
		
		
		public IdentifierArgBeanFactory(ColumnReader<I> reader, SerializableFunction<I, C> factory) {
			this.reader = reader;
			this.factory = factory;
		}
		
		public SerializableFunction<I, C> getFactory() {
			return factory;
		}
		
		@Override
		public C createInstance(ResultSet resultSet) {
			return nullable(readBeanKey(resultSet)).map(this::newInstance).get();
		}
		
		protected I readBeanKey(ResultSet resultSet) {
			return reader.read(resultSet);
		}
		
		protected C newInstance(I beanKey) {
			return factory.apply(beanKey);
		}
		
		@Override
		public IdentifierArgBeanFactory<I, C> copyWithAliases(Function<String, String> columnMapping) {
			return new IdentifierArgBeanFactory<>(reader.copyWithAliases(columnMapping), this.factory);
		}
		
		/**
		 * Used to copy this instance for a subclass of its bean type, required for inheritance implementation.
		 * @param beanFactory subclass one-arg constructor
		 * @param <T> subclass type
		 * @return a new {@link BeanFactory} for a subclass bean which read same column as this instance
		 */
		public <T extends C> IdentifierArgBeanFactory<I, T> copyFor(SerializableFunction<I, T> beanFactory) {
			return new IdentifierArgBeanFactory<>(this.reader, beanFactory);
		}
	}
}

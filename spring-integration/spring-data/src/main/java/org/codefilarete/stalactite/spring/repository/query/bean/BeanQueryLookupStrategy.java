package org.codefilarete.stalactite.spring.repository.query.bean;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.spring.repository.query.BeanQuery;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Predicates;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;

/**
 * {@link QueryLookupStrategy} that tries to detect a query declared via {@link BeanQuery} annotation.
 *
 * @author Guillaume Mary
 */
public class BeanQueryLookupStrategy<C> implements QueryLookupStrategy {
	
	private final ListableBeanFactory beanFactory;
	private final Dialect dialect;
	
	/**
	 * Creates a new {@link BeanQueryLookupStrategy}.
	 *
	 */
	public BeanQueryLookupStrategy(ListableBeanFactory beanFactory,
								   Dialect dialect) {
		this.beanFactory = beanFactory;
		this.dialect = dialect;
	}
	
	@Override
	public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory, NamedQueries namedQueries) {
		BeanQueryMetadata beanQueryMetadata = findBeanQueryMetadata(method);
		if (beanQueryMetadata != null) {
			StalactiteQueryMethod queryMethod = new StalactiteQueryMethod(method, metadata, factory);
			return new BeanRepositoryQuery<>(queryMethod, beanQueryMetadata.getBean(), beanQueryMetadata.getCounterBean(), dialect);
		} else {
			return null;
		}
	}
	
	@VisibleForTesting
	@javax.annotation.Nullable
	BeanQueryMetadata findBeanQueryMetadata(Method method) {
		Set<BeanQueryMetadata> queryMetadataSet = collectBeanQueryMetadata(method);
		switch (queryMetadataSet.size()) {
			case 0:
				return null;
			case 1:
				return Iterables.first(queryMetadataSet);
			default:
				// Checking BeanQuery.repositoryClass
				Set<BeanQueryMetadata> defaultBeans = queryMetadataSet.stream()
						.filter(BeanQueryMetadata::isDefault).collect(Collectors.toSet());
				Set<BeanQueryMetadata> dedicatedBeans = queryMetadataSet.stream()
						.filter(metadata -> metadata.isFor((Class<? extends StalactiteRepository>) method.getDeclaringClass())).collect(Collectors.toSet());
				if (dedicatedBeans.size() == 1) {
					return Iterables.first(dedicatedBeans);
				} else if (defaultBeans.size() == 1) {
					return Iterables.first(defaultBeans);
				}
				throw new UnsupportedOperationException("Multiple beans found matching method " + Reflections.toString(method) + ": "
						+ Iterables.collect(queryMetadataSet, metadata -> metadata.beanName, HashSet::new)
						+ ", but none for repository type " + Reflections.toString(method.getDeclaringClass()) + ": "
						+ Iterables.collect(queryMetadataSet, metadata -> Reflections.toString(metadata.queryAnnotation.repositoryClass()), ArrayList::new)
				);
		}
	}
	
	private Set<BeanQueryMetadata> collectBeanQueryMetadata(Method method) {
		// short class dedicated to local algorithm for @Bean + @BeanQuery metadata storage
		Map<String, Object> beansWithAnnotation = beanFactory.getBeansWithAnnotation(BeanQuery.class);
		Set<BeanQueryMetadata> queryMetadataSet = beansWithAnnotation.entrySet().stream()
				.map(entry -> {
					BeanQuery beanQueryAnnotation = beanFactory.findAnnotationOnBean(entry.getKey(), BeanQuery.class);
					Nullable<ExecutableProjectionQuery> counterBean = Nullable.nullable(beanQueryAnnotation).map(BeanQuery::counterBean)
							.filter(Predicates.not(String::isEmpty))
							.map(counterBeanName -> beanFactory.getBean(counterBeanName, ExecutableProjectionQuery.class));
					return new BeanQueryMetadata(
							entry.getKey(),
							(ExecutableEntityQuery) entry.getValue(),
							counterBean.get(),
							beanQueryAnnotation);
				})
				.filter(metadata ->
						metadata.beanName.equals(method.getName())
								|| Arrays.asList(metadata.queryAnnotation.name()).contains(method.getName())
								|| metadata.queryAnnotation.method().equals(method.getName()))
				.collect(Collectors.toSet());
		return queryMetadataSet;
	}
	
	/**
	 * Internal class to store information about @{@link BeanQuery} while looking for it
	 * @author Guillaume Mary
	 */
	@VisibleForTesting
	static class BeanQueryMetadata {
		private final String beanName;
		private final ExecutableEntityQuery bean;
		private final ExecutableProjectionQuery counterBean;
		private final BeanQuery queryAnnotation;
		
		private BeanQueryMetadata(String beanName, ExecutableEntityQuery bean, ExecutableProjectionQuery counterBean, BeanQuery queryAnnotation) {
			this.beanName = beanName;
			this.bean = bean;
			this.counterBean = counterBean;
			this.queryAnnotation = queryAnnotation;
		}
		
		public ExecutableEntityQuery getBean() {
			return bean;
		}
		
		public ExecutableProjectionQuery getCounterBean() {
			return counterBean;
		}
		
		boolean isDefault() {
			return isFor(StalactiteRepository.class);
		}
		
		boolean isFor(Class<? extends StalactiteRepository> repositoryClass) {
			return queryAnnotation.repositoryClass().equals(repositoryClass);
		}
	}
	
	
}

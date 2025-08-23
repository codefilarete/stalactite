package org.codefilarete.stalactite.spring.repository.query.bean;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.spring.repository.query.BeanQuery;
import org.codefilarete.stalactite.spring.repository.query.StalactiteQueryMethod;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
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
	
	/**
	 * Creates a new {@link BeanQueryLookupStrategy}.
	 *
	 */
	public BeanQueryLookupStrategy(ListableBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}
	
	@Override
	public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory, NamedQueries namedQueries) {
		ExecutableEntityQuery<C, ?> executableEntityQuery = findSQL(method);
		if (executableEntityQuery != null) {
			StalactiteQueryMethod queryMethod = new StalactiteQueryMethod(method, metadata, factory);
			Accumulator<C, ?, ?> accumulator = queryMethod.isCollectionQuery()
					? (Accumulator) Accumulators.toKeepingOrderSet()
					: (Accumulator) Accumulators.getFirstUnique();
			return new BeanRepositoryQuery<>(queryMethod, executableEntityQuery, accumulator);
		} else {
			return null;
		}
	}

	@VisibleForTesting
	@Nullable
	ExecutableEntityQuery<C, ?> findSQL(Method method) {
		// short class dedicated to local algorithm for @Bean + @BeanQuery metadata storage
		class BeanQueryMetadata {
			private final String beanName;
			private final ExecutableEntityQuery bean;
			private final BeanQuery queryAnnotation;

			private BeanQueryMetadata(String beanName, ExecutableEntityQuery bean, BeanQuery queryAnnotation) {
				this.beanName = beanName;
				this.bean = bean;
				this.queryAnnotation = queryAnnotation;
			}

			boolean isDefault() {
				return isFor(StalactiteRepository.class);
			}
			
			boolean isFor(Class<? extends StalactiteRepository> repositoryClass) {
				return queryAnnotation.repositoryClass().equals(repositoryClass);
			}
		}
		
		Map<String, Object> beansWithAnnotation = beanFactory.getBeansWithAnnotation(BeanQuery.class);
		Set<BeanQueryMetadata> queryMetadataSet = beansWithAnnotation.entrySet().stream()
				.map(entry -> new BeanQueryMetadata(
						entry.getKey(),
						(ExecutableEntityQuery) entry.getValue(),
						beanFactory.findAnnotationOnBean(entry.getKey(), BeanQuery.class)))
				.filter(metadata ->
						metadata.beanName.equals(method.getName())
								|| Arrays.asList(metadata.queryAnnotation.name()).contains(method.getName())
								|| metadata.queryAnnotation.method().equals(method.getName()))
				.collect(Collectors.toSet());
		switch (queryMetadataSet.size()) {
			case 0:
				return null;
			case 1:
				return (ExecutableEntityQuery<C, ?>) Iterables.first(queryMetadataSet).bean;
			default:
				// Checking BeanQuery.repositoryClass
				Set<BeanQueryMetadata> defaultBeans = queryMetadataSet.stream()
						.filter(BeanQueryMetadata::isDefault).collect(Collectors.toSet());
				Set<BeanQueryMetadata> dedicatedBeans = queryMetadataSet.stream()
						.filter(metadata -> metadata.isFor((Class<? extends StalactiteRepository>) method.getDeclaringClass())).collect(Collectors.toSet());
				if (dedicatedBeans.size() == 1) {
					return (ExecutableEntityQuery<C, ?>) Iterables.first(dedicatedBeans).bean;
				} else if (defaultBeans.size() == 1) {
					return (ExecutableEntityQuery<C, ?>) Iterables.first(defaultBeans).bean;
				}
				throw new UnsupportedOperationException("Multiple beans found matching method " + Reflections.toString(method) + ": "
						+ Iterables.collect(queryMetadataSet, metadata -> metadata.beanName, HashSet::new)
						+ ", but none for repository type " + Reflections.toString(method.getDeclaringClass()) + ": "
						+ Iterables.collect(queryMetadataSet, metadata -> Reflections.toString(metadata.queryAnnotation.repositoryClass()), ArrayList::new)
				);
		}
	}
}

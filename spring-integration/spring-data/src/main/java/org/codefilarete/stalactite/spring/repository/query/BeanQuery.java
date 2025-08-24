package org.codefilarete.stalactite.spring.repository.query;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.AliasFor;

/**
 * Annotation to be placed on a method to override the default query that is run by a {@link StalactiteRepository}
 * through Spring derived queries.
 * The method must return an instance of {@link org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery}
 * which can be created with an {@link org.codefilarete.stalactite.engine.EntityPersister} instance, which will come
 * from another @{@link Bean}.
 * Here an example of usage:
 * Supposing you have such a repository
 * <pre>{@code
 * public interface MyEntityRepository extends StalactiteRepository<MyEntity, Long> {
 *     MyEntity mySpringDataRepositoryMethodToOverride(String entityName);
 * }
 * }</pre>
 * <pre>{@code
 * @BeanQuery
 * public ExecutableEntityQuery<MyEntity, ?> mySpringDataRepositoryMethodToOverride(EntityPersister<MyEntity, Long> entityPersister) {
 *     return entityPersister.selectWhere(MyEntity::getBooleanProperty, eq(true))
 *                .and(MyEntity::getName, equalsArgNamed("entityName", String.class));
 * }
 * }</pre>
 * In this example the bean will be named "mySpringDataRepositoryMethodToOverride" (default Spring behavior) which is
 * the one of the Stalactite repository whom you want to override the query. This method as an argument named
 * "entityName" which is used by the <code>selectWhere(..)</code> chain.
 * Note that {@link BeanQuery#method()} is not mandatory, in this case the targeted repository method is the one that
 * matches bean name.
 * @author Guillaume Mary
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD })
@Bean
public @interface BeanQuery {
	
	/**
	 * Bean names. Will be used to match the methods to override
	 * @return the bean names
	 */
	@AliasFor(value = "name", annotation = Bean.class)
	String[] value() default {};
	
	/**
	 * Bean names. Will be used to match the methods to override
	 * @return the bean names
	 */
	@AliasFor(value = "value", annotation = Bean.class)
	String[] name() default {};
	
	/**
	 * Repository method name whose query must be overridden.
	 */
	String method() default "";
	
	/**
	 * Repository method name whose query must be overridden.
	 */
	String counterBean() default "";
	
	/**
	 * Repository class name declaring the method to override. Made to overcome the existence of 2 methods with same
	 * names but in 2 different class names. Without it, the method to override would be ambiguous.
	 */
	Class<? extends StalactiteRepository> repositoryClass() default StalactiteRepository.class;
}

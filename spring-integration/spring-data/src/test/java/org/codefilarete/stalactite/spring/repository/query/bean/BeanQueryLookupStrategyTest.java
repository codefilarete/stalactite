package org.codefilarete.stalactite.spring.repository.query.bean;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import org.codefilarete.stalactite.engine.EntityPersister.ExecutableEntityQuery;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.spring.repository.query.BeanQuery;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ListableBeanFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BeanQueryLookupStrategyTest {
	
	@Nested
	class findSQL {

		/** Happy path */
		@Test
		void singleMatchingBeanQueryOnBeanName_returnsMatchingBean() {
			// Method with single matching BeanQuery bean returns valid RepositoryQuery
			ListableBeanFactory beanFactory = mock(ListableBeanFactory.class);
			BeanQueryLookupStrategy<Object> testInstance = new BeanQueryLookupStrategy<>(beanFactory);
			Method repositoryMethod = Reflections.findMethod(DummyRepository.class, "findBestPlayer");
			Method definingBeanMethod = Reflections.findMethod(BeanQueryConfiguration.class, "findBestPlayer");
			BeanQuery beanQuery = definingBeanMethod.getAnnotation(BeanQuery.class);

			ExecutableEntityQuery<Object, ?> expectedQuery = mock(ExecutableEntityQuery.class);
			// note that bean name is the same as repository method one
			Map<String, Object> beans = Maps.asMap("findBestPlayer", expectedQuery);

			when(beanFactory.getBeansWithAnnotation(BeanQuery.class)).thenReturn(beans);
			when(beanFactory.findAnnotationOnBean("bean name doesn't matter", BeanQuery.class)).thenReturn(beanQuery);

			ExecutableEntityQuery<Object, ?> result = testInstance.findSQL(repositoryMethod);

			assertThat(result).isEqualTo(expectedQuery);
		}
		
		/** Happy path */
		@Test
		void singleMatchingBeanQueryOnMethodName_returnsMatchingBean() {
			// Method with single matching BeanQuery bean returns valid RepositoryQuery
			ListableBeanFactory beanFactory = mock(ListableBeanFactory.class);
			BeanQueryLookupStrategy<Object> testInstance = new BeanQueryLookupStrategy<>(beanFactory);
			Method repositoryMethod = Reflections.findMethod(DummyRepository.class, "findBestPlayer");
			Method definingBeanMethod = Reflections.findMethod(BeanQueryConfiguration.class, "queryOverrideForFindBestPlayer");
			BeanQuery beanQuery = definingBeanMethod.getAnnotation(BeanQuery.class);

			ExecutableEntityQuery<Object, ?> expectedQuery = mock(ExecutableEntityQuery.class);
			Map<String, Object> beans = Maps.asMap("bean name doesn't matter", expectedQuery);

			when(beanFactory.getBeansWithAnnotation(BeanQuery.class)).thenReturn(beans);
			when(beanFactory.findAnnotationOnBean("bean name doesn't matter", BeanQuery.class)).thenReturn(beanQuery);

			ExecutableEntityQuery<Object, ?> result = testInstance.findSQL(repositoryMethod);

			assertThat(result).isEqualTo(expectedQuery);
		}

		@Test
		public void multipleMatchingBeanQueries_butOneWithRepositoryClass_returnsMatchingBean() {
			ListableBeanFactory beanFactory = mock(ListableBeanFactory.class);
			BeanQueryLookupStrategy<Object> testInstance = new BeanQueryLookupStrategy<>(beanFactory);
			Method repositoryMethod = Reflections.findMethod(DummyRepository.class, "findBestPlayer");
			// Defining several beans
			Method definingBeanMethod1 = Reflections.findMethod(BeanQueryConfiguration2.class, "queryOverrideForFindBestPlayer");
			BeanQuery beanQuery1 = definingBeanMethod1.getAnnotation(BeanQuery.class);
			Method definingBeanMethod2 = Reflections.findMethod(BeanQueryConfiguration2.class, "anotherQueryOverrideForFindBestPlayer");
			BeanQuery beanQuery2 = definingBeanMethod2.getAnnotation(BeanQuery.class);

			ExecutableEntityQuery<Object, ?> expectedQuery1 = mock(ExecutableEntityQuery.class);
			ExecutableEntityQuery<Object, ?> expectedQuery2 = mock(ExecutableEntityQuery.class);
			Map<String, Object> beans = Maps.forHashMap(String.class, Object.class)
					.add("bean name doesn't matter 1", expectedQuery1)
					.add("bean name doesn't matter 2", expectedQuery2);

			when(beanFactory.getBeansWithAnnotation(BeanQuery.class)).thenReturn(beans);
			when(beanFactory.findAnnotationOnBean("bean name doesn't matter 1", BeanQuery.class)).thenReturn(beanQuery1);
			when(beanFactory.findAnnotationOnBean("bean name doesn't matter 2", BeanQuery.class)).thenReturn(beanQuery2);
			
			ExecutableEntityQuery<Object, ?> result = testInstance.findSQL(repositoryMethod);
			
			assertThat(result).isEqualTo(expectedQuery2);
		}

		@Test
		public void multipleMatchingBeanQueries_throwsException() {
			ListableBeanFactory beanFactory = mock(ListableBeanFactory.class);
			BeanQueryLookupStrategy<Object> testInstance = new BeanQueryLookupStrategy<>(beanFactory);
			Method repositoryMethod = Reflections.findMethod(DummyRepository.class, "findBestPlayer");
			// Defining several beans
			Method definingBeanMethod1 = Reflections.findMethod(BeanQueryConfiguration.class, "queryOverrideForFindBestPlayer");
			BeanQuery beanQuery1 = definingBeanMethod1.getAnnotation(BeanQuery.class);
			Method definingBeanMethod2 = Reflections.findMethod(BeanQueryConfiguration.class, "anotherQueryOverrideForFindBestPlayer");
			BeanQuery beanQuery2 = definingBeanMethod2.getAnnotation(BeanQuery.class);

			ExecutableEntityQuery<Object, ?> expectedQuery1 = mock(ExecutableEntityQuery.class);
			ExecutableEntityQuery<Object, ?> expectedQuery2 = mock(ExecutableEntityQuery.class);
			Map<String, Object> beans = Maps.forHashMap(String.class, Object.class)
					.add("bean name doesn't matter 1", expectedQuery1)
					.add("bean name doesn't matter 2", expectedQuery2);

			when(beanFactory.getBeansWithAnnotation(BeanQuery.class)).thenReturn(beans);
			when(beanFactory.findAnnotationOnBean("bean name doesn't matter 1", BeanQuery.class)).thenReturn(beanQuery1);
			when(beanFactory.findAnnotationOnBean("bean name doesn't matter 2", BeanQuery.class)).thenReturn(beanQuery2);

			assertThatCode(() -> testInstance.findSQL(repositoryMethod))
					.isInstanceOf(UnsupportedOperationException.class)
					.hasMessage("Multiple beans found matching method o.c.s.s.r.q.b.BeanQueryLookupStrategyTest$DummyRepository.findBestPlayer(): "
							+ "[bean name doesn't matter 1, bean name doesn't matter 2]"
							+ ", but none for repository type o.c.s.s.r.q.b.BeanQueryLookupStrategyTest$DummyRepository: [o.c.s.s.r.StalactiteRepository, o.c.s.s.r.StalactiteRepository]");
		}

		@Test
		public void noMatchingBeanQueries_returnsNull() {
			ListableBeanFactory beanFactory = mock(ListableBeanFactory.class);
			BeanQueryLookupStrategy<Object> testInstance = new BeanQueryLookupStrategy<>(beanFactory);
			Method repositoryMethod = Reflections.findMethod(DummyRepository.class, "findBestPlayer");
			// Defining no beans
			Map<String, Object> beans = Collections.emptyMap();
			when(beanFactory.getBeansWithAnnotation(BeanQuery.class)).thenReturn(beans);

			assertThat(testInstance.findSQL(repositoryMethod)).isNull();
		}
	}

	public interface DummyRepository extends StalactiteRepository<Object, Object> {

		Object findBestPlayer();
	}
	
	public static class BeanQueryConfiguration {
		
		@BeanQuery
		public ExecutableEntityQuery findBestPlayer() {
			return mock(ExecutableEntityQuery.class);
		}

		@BeanQuery(method = "findBestPlayer")
		public ExecutableEntityQuery queryOverrideForFindBestPlayer() {
			return mock(ExecutableEntityQuery.class);
		}

		@BeanQuery(method = "findBestPlayer")
		public ExecutableEntityQuery anotherQueryOverrideForFindBestPlayer() {
			return mock(ExecutableEntityQuery.class);
		}
	}

	public static class BeanQueryConfiguration2 {

		@BeanQuery(method = "findBestPlayer")
		public ExecutableEntityQuery queryOverrideForFindBestPlayer() {
			return mock(ExecutableEntityQuery.class);
		}

		@BeanQuery(method = "findBestPlayer", repositoryClass = DummyRepository.class)
		public ExecutableEntityQuery anotherQueryOverrideForFindBestPlayer() {
			return mock(ExecutableEntityQuery.class);
		}
	}
}

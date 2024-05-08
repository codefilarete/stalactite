package org.codefilarete.stalactite.sql.spring.repository.config;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import org.codefilarete.stalactite.sql.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.sql.spring.repository.StalactiteRepositoryFactoryBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.DefaultRepositoryConfiguration;
import org.springframework.data.repository.config.RepositoryConfiguration;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Stalactite specific configuration extension parsing custom attributes from the XML namespace and
 * {@link EnableJpaRepositories} annotation.
 * Implementation is inspired by {@link org.springframework.data.jpa.repository.config.JpaRepositoryConfigExtension}
 * which was taken as an example.
 * 
 * @author Guillaume Mary
 */
public class StalactiteRepositoryConfigExtension extends RepositoryConfigurationExtensionSupport {
	
	private static final String DEFAULT_TRANSACTION_MANAGER_BEAN_NAME = "transactionManager";
	private static final String ENABLE_DEFAULT_TRANSACTIONS_ATTRIBUTE = "enableDefaultTransactions";
	
	
	@Override
	public String getModuleName() {
		return "Stalactite";
	}

	@Override
	public String getRepositoryFactoryBeanClassName() {
		return StalactiteRepositoryFactoryBean.class.getName();
	}

	@Override
	protected String getModulePrefix() {
		return getModuleName().toLowerCase(Locale.US);
	}

	/* No annotations with Stalactite, so no reason to scan for entities
	@Override
	protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
		return Arrays.asList(Entity.class, MappedSuperclass.class);
	}
	*/

	@Override
	protected Collection<Class<?>> getIdentifyingTypes() {
		return Collections.singleton(StalactiteRepository.class);
	}
	
	@Override
	public void postProcess(BeanDefinitionBuilder builder, RepositoryConfigurationSource source) {
		Optional<String> transactionManagerRef = source.getAttribute("transactionManagerRef");
		builder.addPropertyValue("transactionManager", transactionManagerRef.orElse(DEFAULT_TRANSACTION_MANAGER_BEAN_NAME));
	}

	@Override
	public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {

		AnnotationAttributes attributes = config.getAttributes();

		builder.addPropertyValue(ENABLE_DEFAULT_TRANSACTIONS_ATTRIBUTE,
				attributes.getBoolean(ENABLE_DEFAULT_TRANSACTIONS_ATTRIBUTE));
	}

	@Override
	public void postProcess(BeanDefinitionBuilder builder, XmlRepositoryConfigurationSource config) {

		Optional<String> enableDefaultTransactions = config.getAttribute(ENABLE_DEFAULT_TRANSACTIONS_ATTRIBUTE);

		if (enableDefaultTransactions.isPresent() && StringUtils.hasText(enableDefaultTransactions.get())) {
			builder.addPropertyValue(ENABLE_DEFAULT_TRANSACTIONS_ATTRIBUTE, enableDefaultTransactions.get());
		}
	}

	@Override
	protected ClassLoader getConfigurationInspectionClassLoader(ResourceLoader loader) {

		ClassLoader classLoader = loader.getClassLoader();

		return classLoader != null && LazyJvmAgent.isActive(loader.getClassLoader())
				? new InspectionClassLoader(loader.getClassLoader())
				: loader.getClassLoader();
	}
	
	/**
	 * Overridden to adapt the configSource argument to Stalactite case and ovoid exception throwing : some expected
	 * options are not available in {@link EnableStalactiteRepositories} but {@link AnnotationRepositoryConfigurationSource}
	 * doesn't handle their missing correctly and throws an exception.
	 * 
	 * @param definition the definition of the bean-repository being created
	 * @param configSource a {@link AnnotationRepositoryConfigurationSource} that gives access to the real {@link EnableStalactiteRepositories} info
	 * @return a {@link DefaultRepositoryConfiguration}
	 * @param <T> type of configuration source, for us a {@link AnnotationRepositoryConfigurationSource}
	 */
	@Override
	protected <T extends RepositoryConfigurationSource> RepositoryConfiguration<T> getRepositoryConfiguration(
			BeanDefinition definition, T configSource) {
		// Note that we can cast given argument to AnnotationRepositoryConfigurationSource because that's what is
		// instantiated in RepositoryBeanDefinitionRegistrarSupport.registerBeanDefinitions(..) which is the code
		// that invokes us with different intermediaries.
		T repositoryConfigurationSource = (T) new EnableStalactiteRepositoriesRepositoryConfigurationSource((AnnotationRepositoryConfigurationSource) configSource);
		return new DefaultRepositoryConfiguration<>(repositoryConfigurationSource, definition, this);
	}

	/**
	 * Utility to determine if a lazy Java agent is being used that might transform classes at a later time.
	 */
	static class LazyJvmAgent {

		private static final Set<String> AGENT_CLASSES;

		static {

			Set<String> agentClasses = new LinkedHashSet<>();

			agentClasses.add("org.springframework.instrument.InstrumentationSavingAgent");
			agentClasses.add("org.eclipse.persistence.internal.jpa.deployment.JavaSECMPInitializerAgent");

			AGENT_CLASSES = Collections.unmodifiableSet(agentClasses);
		}

		private LazyJvmAgent() {}

		/**
		 * Determine if any agent is active.
		 *
		 * @return {@literal true} if an agent is active.
		 */
		static boolean isActive(@Nullable ClassLoader classLoader) {

			return AGENT_CLASSES.stream() //
					.anyMatch(agentClass -> ClassUtils.isPresent(agentClass, classLoader));
		}
	}
}

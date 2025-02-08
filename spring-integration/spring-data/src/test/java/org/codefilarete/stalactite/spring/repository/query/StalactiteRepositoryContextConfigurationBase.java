package org.codefilarete.stalactite.spring.repository.query;

import javax.sql.DataSource;

import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.CurrentThreadTransactionalConnectionProvider;
import org.codefilarete.stalactite.engine.model.Color;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.HSQLDBDialect;
import org.codefilarete.stalactite.sql.ddl.DDLDeployer;
import org.codefilarete.stalactite.spring.transaction.StalactitePlatformTransactionManager;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.LambdaParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.NullAwareParameterBinder;
import org.codefilarete.stalactite.sql.test.HSQLDBInMemoryDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

import static org.codefilarete.stalactite.id.Identifier.identifierBinder;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders.INTEGER_PRIMITIVE_BINDER;

public class StalactiteRepositoryContextConfigurationBase {
	
	@Bean
	public DataSource dataSource() {
		return new HSQLDBInMemoryDataSource();
	}
	
	@Bean
	public StalactitePlatformTransactionManager transactionManager(DataSource dataSource) {
		return new StalactitePlatformTransactionManager(dataSource);
	}
	
	@Bean
	public PersistenceContext persistenceContext(StalactitePlatformTransactionManager dataSource) {
		HSQLDBDialect dialect = new HSQLDBDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		
		dialect.getColumnBinderRegistry().register(Color.class, new NullAwareParameterBinder<>(new LambdaParameterBinder<>(INTEGER_PRIMITIVE_BINDER, Color::new, Color::getRgb)));
		dialect.getSqlTypeRegistry().put(Color.class, "int");
		
		return new PersistenceContext(dataSource, dialect);
	}
	
	@EventListener
	public void onApplicationEvent(ContextRefreshedEvent event) {
		PersistenceContext persistenceContext = event.getApplicationContext().getBean(PersistenceContext.class);
		DataSource dataSource = event.getApplicationContext().getBean(DataSource.class);
		Dialect dialect = persistenceContext.getDialect();
		DDLDeployer ddlDeployer = new DDLDeployer(dialect.getDdlTableGenerator(), new CurrentThreadTransactionalConnectionProvider(dataSource));
		ddlDeployer.getDdlGenerator().addTables(DDLDeployer.collectTables(persistenceContext));
		ddlDeployer.deployDDL();
	}
}
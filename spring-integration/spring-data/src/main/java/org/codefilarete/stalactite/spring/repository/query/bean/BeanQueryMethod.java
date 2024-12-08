package org.codefilarete.stalactite.spring.repository.query.bean;

import java.lang.reflect.Method;

import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;

public class BeanQueryMethod extends QueryMethod {
	
	public BeanQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory) {
		super(method, metadata, factory);
	}
	
	@Override
	protected RelationalParameters createParameters(Method method) {
		return new RelationalParameters(method);
	}
	
	@Override
	public RelationalParameters getParameters() {
		return (RelationalParameters) super.getParameters();
	}
}

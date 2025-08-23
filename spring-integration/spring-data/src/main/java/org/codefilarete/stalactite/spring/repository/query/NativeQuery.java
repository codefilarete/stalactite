package org.codefilarete.stalactite.spring.repository.query;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.QueryAnnotation;

/**
 * Annotation to declare a SQL query on repository methods.
 *
 * @author Guillaume Mary
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@QueryAnnotation
@Documented
@Repeatable(NativeQueries.class)
public @interface NativeQuery {
	
	/**
	 * Defines the SQL query to be executed when the annotated method is called.
	 */
	String value() default "";

	/**
	 * Defines the SQL count query to be executed when the annotated method return type is a {@link org.springframework.data.domain.Page}.
	 */
	String countQuery() default "";

	/**
	 * Database vendor compatibility of current query. Default is empty which means that it will be applied to all
	 * databases
	 */
	String vendor() default "";

	int major() default 0;

	int minor() default 0;
}

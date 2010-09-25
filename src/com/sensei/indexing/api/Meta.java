package com.sensei.indexing.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Meta {
	String name() default "";
	MetaType type() default MetaType.String;
	String format() default "";
}

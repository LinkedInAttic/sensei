package com.sensei.indexing.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;


@Target(ElementType.FIELD)
public @interface Text {
	String name() default "";
	Store store() default Store.NO;
	Index index() default Index.ANALYZED;
	TermVector termVector() default TermVector.NO;
}

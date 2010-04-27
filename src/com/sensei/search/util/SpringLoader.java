package com.sensei.search.util;

import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 *
 * Simple application to load up a context file
 *
 * @author Brian Hammond
 *
 */
public class SpringLoader {
	public static void main( String args[] ) throws Exception {
		new FileSystemXmlApplicationContext( args );
	}
};

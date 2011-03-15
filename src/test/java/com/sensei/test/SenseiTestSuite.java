package com.sensei.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class SenseiTestSuite extends TestSuite{
  public static Test suite(){
     TestSuite suite=new TestSuite();
     suite.addTestSuite(TestSensei.class);
     suite.addTestSuite(TestSerialization.class);
     suite.addTestSuite(TestIndexingAPI.class);
     suite.addTestSuite(TestRestServer.class);
     suite.addTestSuite(TestUIDFacetHandlerUsingBoboZoie.class);
     return suite;
  }
 
  /**
   * @param args
   */
  public static void main(String[] args) {
         TestRunner.run(suite());
  }
}

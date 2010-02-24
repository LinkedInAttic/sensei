package com.sensei.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class SenseiTestSuite extends TestSuite{
  public static Test suite(){
     TestSuite suite=new TestSuite();
     suite.addTestSuite(SenseiTestCase.class);
     return suite;
  }
 
  /**
   * @param args
   */
  public static void main(String[] args) {
         TestRunner.run(suite());
  }
}

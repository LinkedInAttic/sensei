package com.senseidb.plugin;

import java.util.HashSet;
import java.util.Map;

import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.senseidb.util.RequestConverter;

public class TestAbstractSenseiPlugin extends TestCase{

	public TestAbstractSenseiPlugin(String name){
		super(name);
	}
	
    /**
     * A very basic test, mostly to force me to write tests.
     */
	public void testInitParamAssertion() throws Exception{
        SenseiPluginRegistry reg = SenseiPluginRegistry.build(new BaseConfiguration());
        Map<String, String> conf = Maps.newHashMap();
        SenseiPlugin p = new ConcreteSenseiPlugin();
        //this should work
        p.init(conf, reg);

        p = new ConcreteSenseiPlugin();
        //these cases shouldn't work
        try {
            p.init(null, reg);
            assertTrue("Failed to fail on null on the first param.", false);
        } catch (NullPointerException e) {}

        p = new ConcreteSenseiPlugin();
        try {
            p.init(conf, null);
            assertTrue("Failed to fail on null on the second param.", false);
        } catch (NullPointerException e) {}
	}

    public void testStateTransitionAssertions() {
        SenseiPluginRegistry reg = SenseiPluginRegistry.build(new BaseConfiguration());
        Map<String, String> conf = Maps.newHashMap();
        
        //this should work
        SenseiPlugin p = new ConcreteSenseiPlugin();
        p.init(conf, reg);
        p.start();
        p.stop();

        //this shuldn't
        p = new ConcreteSenseiPlugin();
        p.init(conf, reg);
        try {
            p.stop();
            assertTrue("Stop without start should fail", false);
        } catch (IllegalStateException e) {}

        //these shouldn't
        p = new ConcreteSenseiPlugin();
        try {
            p.start();
            assertTrue("Start without init should fail", false);
        } catch (IllegalStateException e) {}

        p = new ConcreteSenseiPlugin();
        try {
            p.stop();
            assertTrue("Stop without init should fail.", false);
        } catch (IllegalStateException e) {}

        p = new ConcreteSenseiPlugin();
        p.init(conf, reg);
        try {
            p.init(conf, reg);
            assertTrue("Double init should fail.", false);
        } catch (IllegalStateException e) {}

        p = new ConcreteSenseiPlugin();
        p.init(conf, reg);
        p.start();
        try {
            p.start();
            assertTrue("Double start should fail.", false);
        } catch (IllegalStateException e) {}

        p = new ConcreteSenseiPlugin();
        p.init(conf, reg);
        p.start();
        p.stop();
        try {
            p.stop();
            assertTrue("Double stop should fail.", false);
        } catch (IllegalStateException e) {}
    }

    private static class ConcreteSenseiPlugin extends AbstractSenseiPlugin {
    }
}

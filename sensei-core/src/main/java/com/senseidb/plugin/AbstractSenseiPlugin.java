package com.senseidb.plugin;

import java.util.Map;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * An AbstractSenseiPlugin provides a default init() that saves the references
 * passed to the configuration and the plugin registry to have them available for use
 * during the start(). 
 */
public abstract class AbstractSenseiPlugin implements SenseiPlugin {
    protected Map<String, String> config;
    protected SenseiPluginRegistry pluginRegistry;
    private static enum State {CONSTRUCTED, INITIALIZED, STARTED, STOPPED}
    private State state = State.CONSTRUCTED; 

    @Override
    public final synchronized void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
        checkState(state == State.CONSTRUCTED, "Called init while in state %s.", state);
        this.config= checkNotNull(config);
        this.pluginRegistry = checkNotNull(pluginRegistry);
        internalInit();
        state = State.INITIALIZED;
    }

    @Override
    public final synchronized void start() {
        checkState(state == State.INITIALIZED, "Called start while in state %s.", state);
        internalStart();
        state = State.STARTED;
    }

    @Override
    public final synchronized void stop() {
        checkState(state == State.STARTED, "Called stop while in state %s.", state);
        internalStop();
        state = State.STOPPED;
    }

    /**
     * Overridable init method.
     * Will be called by init after checking the current state.
     * Synchronization (to this) is already provided by init().
     */
    protected void internalInit() {}

    /**
     * Overridable start method.
     * Will be called by start() after checking the current state.
     * Synchronization (to this) is already provided by start().
     */
    protected void internalStart() {}

    /**
     * Overridable stop method.
     * Will be called bi stop() after checking the current state.
     * Synchronization (to this) is already provided by stop().
     */
    protected void internalStop() {}

    
}

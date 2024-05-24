/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;


/**
 * Options of the Horse
 */
public class HorseOptions {


    public static final boolean ENABLE_HORSE = false;
    private volatile boolean enableHorse = ENABLE_HORSE;


    /**
     * Creates a new {@code ProtocolOptions} instance using the {@code DEFAULT_PORT}
     * (and without SSL).
     */
    public HorseOptions() {
    }

    /**
     * Returns whether the horse is enabled.
     *
     * @return whether the horse is enabled.
     */
    public boolean isHorseEnabled() {
        return enableHorse;
    }

    /**
     * Setup the horse.
     *
     * @param enableHorse whether to enable the horse.
     */
    public HorseOptions setHorse(boolean enableHorse) {
        this.enableHorse = enableHorse;
        return this;
    }

    

}

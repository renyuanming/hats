package com.datastax.driver.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HorseUtils {

    
    private static final Logger logger = LoggerFactory.getLogger(HorseUtils.class);

    public enum HorseLogLevels {
        TRACE, 
        DEBUG, 
        INFO, 
        WARN, 
        ERROR
    }

    public static void printStackTace(HorseLogLevels logLevel, String msg) 
    {
        if (logLevel.equals(HorseLogLevels.DEBUG))
            logger.debug("stack trace {}", new Exception(msg));
        if (logLevel.equals(HorseLogLevels.ERROR))
            logger.error("stack trace {}", new Exception(msg));
        if (logLevel.equals(HorseLogLevels.INFO))
            logger.info("stack trace {}", new Exception(msg));
    }
}

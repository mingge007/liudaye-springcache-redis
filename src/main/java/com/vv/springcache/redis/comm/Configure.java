package com.vv.springcache.redis.comm;

import java.util.Properties;

public class Configure {

    private Properties properties = null;

    public int getIntVal(String key, int def) {
        String val = properties.getProperty(key);
        try {
            return Integer.parseInt(val);
        } catch (Exception e) {
            return def;
        }
    }

    public long getLongVal(String key, long def) {
        String val = properties.getProperty(key);
        try {
            return Long.parseLong(val);
        } catch (Exception e) {
            return def;
        }
    }

    public String getStrVal(String key, String def) {
        return properties.getProperty(key, def);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }


}

package it.polimi.ds;

import java.util.Date;

public class Value {
    
    public static class Version {
        private final long version;
        private final long timestamp;

        public Version(long version) {
            this.version = version;
            this.timestamp = new Date().getTime();
        }

        public boolean greaterThan(Version other) {
            if (this.version == other.version) {
                return this.timestamp > other.timestamp;
            }
            return this.version > other.version;
        }

        public long getVersion() {
            return this.version;
        }

        public long getTimestamp() {
            return this.timestamp;
        }
    }

    private final Version version;
    private final String value;

    public Value(Version version, String value) {
        this.version = version;
        this.value = value;
    }

    public Version getVersion() {
        return this.version;
    }

    public String getValue() {
        return this.value;
    }
}

package it.polimi.ds;

import java.io.Serializable;
import java.util.Random;

public class Value implements Serializable {
    
    public static class Versioning implements Serializable{
        private final long version;
        private final long timestamp;

        public Versioning(long version) {
            this.version = version;
            this.timestamp = new Random().nextLong();
        }

        public boolean greaterThan(Versioning other) {
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

    private final Versioning versioning;
    private final String value;

    public Value(Versioning versioning, String value) {
        this.versioning = versioning;
        this.value = value;
    }

    public Versioning getVersioning() {
        return this.versioning;
    }

    public String getValue() {
        return this.value;
    }
}

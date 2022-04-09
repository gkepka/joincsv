package app.utils;

@FunctionalInterface
public interface HashFunction {
    int getHash(String value);
}

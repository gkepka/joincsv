package app.joins;

@FunctionalInterface
public interface HashFunction {
    int getHash(String value);
}

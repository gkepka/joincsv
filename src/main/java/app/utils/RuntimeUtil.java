package app.utils;

public class RuntimeUtil {

    public static long getTotalFreeMemory() {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long totalMemory = Runtime.getRuntime().totalMemory();
        long usedMemory = totalMemory - Runtime.getRuntime().freeMemory();
        return maxMemory - usedMemory;

    }
}

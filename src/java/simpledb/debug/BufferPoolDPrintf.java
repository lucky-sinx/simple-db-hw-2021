package simpledb.debug;

public class BufferPoolDPrintf {
    private static final boolean debug = false;

    public static void print(String s) {
        if (debug) {
            System.out.println(s);
        }
    }
}

package io.krkafka.loader;

/** Standalone subprocess target: intentionally has no JUnit dependency. */
public final class LoaderProbe {
    public static void main(String[] args) {
        try {
            NativeLibrary.load();
            NativeLibrary.load();
            System.out.println("ABI_OK");
        } catch (Throwable failure) {
            System.err.println(failure.getMessage());
            System.exit(17);
        }
    }
}

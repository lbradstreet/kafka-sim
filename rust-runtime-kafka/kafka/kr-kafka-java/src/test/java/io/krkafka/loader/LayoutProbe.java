package io.krkafka.loader;

/** Standalone entry for running the complete C/FFM layout test on each release target. */
public final class LayoutProbe {
    public static void main(String[] args) throws Exception {
        new LayoutsTest().everyGeneratedLayoutAndFieldMatchesTheCCompiler();
        System.out.println("LAYOUTS_OK arch=" + System.getProperty("os.arch"));
    }
}

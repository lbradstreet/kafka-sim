package io.krkafka.loader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NativeLibraryTest {
    record Result(int code, String text) {}
    @Test void unsupportedPlatformsHaveActionableErrors() {
        assertTrue(assertThrows(UnsupportedOperationException.class,
            () -> NativeLibrary.platform("Windows", "amd64")).getMessage().contains("Linux"));
        assertTrue(assertThrows(UnsupportedOperationException.class,
            () -> NativeLibrary.platform("Linux", "riscv64")).getMessage().contains("architecture"));
    }

    @Test void packagedBytesRequireMatchingChecksumBeforeExtraction() throws Exception {
        Path root = Files.createTempDirectory("kr-java-packaged-checksum-");
        Path nativeDirectory = root.resolve("META-INF/native/linux-x86_64");
        Files.createDirectories(nativeDirectory);
        Path library = nativeDirectory.resolve("libkr_kafka_ffi.so");
        Path checksum = nativeDirectory.resolve("libkr_kafka_ffi.so.sha256");
        byte[] bytes = {1, 2, 3, 4};
        Files.write(library, bytes);
        Files.writeString(checksum, "0".repeat(64));
        List<java.net.URL> locations = new ArrayList<>();
        locations.add(root.toUri().toURL());
        for (String path : System.getProperty("kr.kafka.test.classpath").split(java.util.regex.Pattern.quote(java.io.File.pathSeparator)))
            locations.add(Path.of(path).toUri().toURL());
        try (var loader = new java.net.URLClassLoader(locations.toArray(java.net.URL[]::new), null)) {
            // A separate defining loader owns these controlled bundle resources.
            var type = Class.forName(NativeLibrary.class.getName(), true, loader);
            var extract = type.getDeclaredMethod("extract", String.class);
            extract.setAccessible(true);
            var failure = assertThrows(java.lang.reflect.InvocationTargetException.class,
                () -> extract.invoke(null, "linux-x86_64"));
            assertTrue(failure.getCause().getMessage().contains("checksum mismatch"));
            Files.writeString(checksum, java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)));
            Path extracted = (Path) extract.invoke(null, "linux-x86_64");
            try {
                assertArrayEquals(bytes, Files.readAllBytes(extracted));
                assertEquals(java.nio.file.attribute.PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(extracted.getParent()));
                assertEquals(java.nio.file.attribute.PosixFilePermissions.fromString("r-x------"), Files.getPosixFilePermissions(extracted));
            } finally { Files.delete(extracted); Files.delete(extracted.getParent()); }
        } finally {
            Files.delete(checksum); Files.delete(library); Files.delete(nativeDirectory);
            Files.delete(nativeDirectory.getParent()); Files.delete(root.resolve("META-INF")); Files.delete(root);
        }
    }

    private Result probe(String library, boolean nativeAccess, boolean modulePath) throws Exception {
        var args = new ArrayList<String>();
        args.add(Path.of(System.getProperty("java.home"), "bin/java").toString());
        if (nativeAccess) args.add("--enable-native-access=" + (modulePath ? "io.krkafka" : "ALL-UNNAMED"));
        args.add("-Dkr.kafka.library=" + library);
        if (modulePath) {
            args.addAll(List.of("--module-path", System.getProperty("kr.kafka.test.modulepath"),
                "--patch-module", "io.krkafka=" + Path.of("build/classes/java/test").toAbsolutePath()
                    + java.io.File.pathSeparator + Path.of("build/resources/main").toAbsolutePath(),
                "--module", "io.krkafka/io.krkafka.loader.LoaderProbe"));
        } else args.addAll(List.of("-cp", System.getProperty("kr.kafka.test.classpath"), LoaderProbe.class.getName()));
        var process = new ProcessBuilder(args).redirectErrorStream(true).start();
        try {
            assertTrue(process.waitFor(20, TimeUnit.SECONDS), "loader probe exceeded bound");
            return new Result(process.exitValue(), new String(process.getInputStream().readAllBytes()));
        } finally { if (process.isAlive()) process.destroyForcibly().waitFor(); }
    }

    @Test void matchingAbiLoadsFromClasspathAndModulePath() throws Exception {
        for (boolean module : new boolean[] {false, true}) {
            var result = probe(System.getProperty("kr.kafka.library"), true, module);
            assertEquals(0, result.code, result.text);
            assertTrue(result.text.contains("ABI_OK"));
        }
    }

    @Test void nativeAccessIsRequiredBeforeLibraryLoading() throws Exception {
        for (boolean module : new boolean[] {false, true}) {
            var result = probe("/nonexistent/never-loaded.so", false, module);
            assertEquals(17, result.code, result.text);
            assertTrue(result.text.contains("Enable native access"), result.text);
            assertFalse(result.text.contains("ABI_OK"));
        }
    }

    @Test void rejectsRelativeMissingWrongAndSymbolFreeLibraries() throws Exception {
        var relative = probe("relative.so", true, false);
        assertEquals(17, relative.code);
        assertTrue(relative.text.contains("absolute path"));
        var missing = probe("/nonexistent/libkr_kafka_ffi.so", true, false);
        assertEquals(17, missing.code);
        for (int version : new int[] {0, 1, 2, 3, 4, -1}) {
            Path directory = Files.createTempDirectory("kr-java-wrong-abi-");
            Path source = directory.resolve("wrong.c");
            Path library = directory.resolve(System.mapLibraryName("wrong"));
            try {
                String c = "#include <stdint.h>\n#include <stdlib.h>\n"
                    + (version == 0 ? "int unrelated(void) {return 0;}\n" : "uint32_t kr_abi_version(void) {return " + version + ";}\n")
                    + "int kr_producer_config_init(void *p, unsigned n) {abort();}\n"
                    + "int kr_producer_create(void *p, void *q) {abort();}\n";
                Files.writeString(source, c);
                var compiler = new ProcessBuilder("cc", "-shared", "-fPIC", "-Werror", source.toString(), "-o", library.toString()).redirectErrorStream(true).start();
                String diagnostics = new String(compiler.getInputStream().readAllBytes());
                assertEquals(0, compiler.waitFor(), diagnostics);
                for (boolean module : new boolean[] {false, true}) {
                    var result = probe(library.toString(), true, module);
                    assertEquals(17, result.code, "constructor must never run: " + result.text);
                    assertTrue(result.text.contains(version == 0 ? "Missing kr_abi_version"
                        : version == 4 ? "Missing native ABI symbol" : "ABI mismatch"), result.text);
                }
            } finally {
                Files.deleteIfExists(library); Files.deleteIfExists(source); Files.deleteIfExists(directory);
            }
        }
    }
}

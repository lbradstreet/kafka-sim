package io.krkafka.loader;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;
import org.apache.kafka.common.KafkaException;

/** Process-lifetime System.load arrangement shared by every generated downcall. */
public final class NativeLibrary {
    public static final int ABI_VERSION = 4;
    private static Path loaded;
    private static KafkaException failure;
    private NativeLibrary() {}

    public static synchronized void load() {
        if (failure != null) throw failure;
        if (loaded != null) return;
        try {
            // This check precedes System.load and any generated class initialization.
            if (!NativeLibrary.class.getModule().isNativeAccessEnabled()) {
                throw new IllegalStateException("Enable native access with --enable-native-access="
                    + (NativeLibrary.class.getModule().isNamed() ? "io.krkafka" : "ALL-UNNAMED"));
            }
            String override = System.getProperty("kr.kafka.library");
            Path path;
            if (override != null) {
                path = Path.of(override);
                if (!path.isAbsolute()) throw new IllegalArgumentException("kr.kafka.library must be an absolute path");
                path = path.toRealPath();
            } else {
                path = extract(platform(System.getProperty("os.name"), System.getProperty("os.arch")));
            }
            System.load(path.toString());
            var symbol = SymbolLookup.loaderLookup().find("kr_abi_version")
                .orElseThrow(() -> new UnsatisfiedLinkError("Missing kr_abi_version"));
            var version = Linker.nativeLinker().downcallHandle(symbol, FunctionDescriptor.of(ValueLayout.JAVA_INT));
            int actual = (int) version.invokeExact();
            if (actual != ABI_VERSION) throw new UnsatisfiedLinkError(
                "Native ABI mismatch: expected " + ABI_VERSION + ", found " + Integer.toUnsignedString(actual));
            // Resolve the complete generated inventory before creation can give
            // Java a handle. In particular, a missing destroy must never strand
            // a successfully created native owner behind a lazy linkage error.
            try (InputStream manifest = NativeLibrary.class.getResourceAsStream("/META-INF/kr-kafka-symbols.txt")) {
                if (manifest == null) throw new IOException("Missing native ABI symbol inventory");
                String names = new String(manifest.readAllBytes(), java.nio.charset.StandardCharsets.US_ASCII);
                var lookup = SymbolLookup.loaderLookup();
                for (String name : names.lines().toList()) {
                    if (!name.matches("kr_[a-z_]+") || lookup.find(name).isEmpty())
                        throw new UnsatisfiedLinkError("Missing native ABI symbol: " + name);
                }
            }
            loaded = path;
        } catch (Throwable cause) {
            if (cause instanceof VirtualMachineError error) throw error;
            failure = new KafkaException("Cannot load kr-kafka native library: " + cause.getMessage(), cause);
            throw failure;
        }
    }

    static String platform(String os, String arch) {
        if (!os.toLowerCase(Locale.ROOT).equals("linux")) {
            throw new UnsupportedOperationException("Bundled native startup requires Linux glibc 2.28+; "
                + "use an absolute kr.kafka.library for development ABI tests");
        }
        String target = switch (arch.toLowerCase(Locale.ROOT)) {
            case "amd64", "x86_64" -> "linux-x86_64";
            case "aarch64", "arm64" -> "linux-aarch64";
            default -> throw new UnsupportedOperationException("Unsupported native architecture: " + arch);
        };
        // musl's loader cannot satisfy the packaged GNU ABI. The dynamic loader
        // additionally enforces the binary's versioned glibc symbol baseline.
        Path loader = Path.of(target.endsWith("x86_64") ? "/lib64/ld-linux-x86-64.so.2" : "/lib/ld-linux-aarch64.so.1");
        if (!Files.exists(loader)) throw new UnsupportedOperationException("Bundled native library requires glibc 2.28+ (musl is unsupported)");
        return target;
    }

    private static Path extract(String platform) throws Exception {
        String base = "/META-INF/native/" + platform + "/libkr_kafka_ffi.so";
        byte[] bytes;
        String expected;
        try (InputStream lib = NativeLibrary.class.getResourceAsStream(base);
             InputStream hash = NativeLibrary.class.getResourceAsStream(base + ".sha256")) {
            if (lib == null || hash == null) throw new IOException("No packaged " + platform
                + " artifact; build the matching native release and set -Dkr.kafka.library=<absolute path>");
            bytes = lib.readAllBytes();
            expected = new String(hash.readAllBytes(), java.nio.charset.StandardCharsets.US_ASCII).trim();
        }
        String actual = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        if (!actual.equals(expected)) throw new IOException("Packaged native checksum mismatch");
        Path directory = Files.createTempDirectory("kr-kafka-", PosixFilePermissions.asFileAttribute(
            PosixFilePermissions.fromString("rwx------")));
        Path path = directory.resolve("libkr_kafka_ffi.so");
        try {
            Files.write(path, bytes);
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("r-x------"));
            // The JVM retains the loaded image. Keep the pathname until normal process exit.
            directory.toFile().deleteOnExit();
            path.toFile().deleteOnExit();
            return path.toRealPath();
        } catch (Throwable cause) {
            Files.deleteIfExists(path);
            Files.deleteIfExists(directory);
            throw cause;
        }
    }
}

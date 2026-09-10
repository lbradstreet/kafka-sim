package io.krkafka.loader;

import java.lang.foreign.MemoryLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LayoutsTest {
    @Test void everyGeneratedLayoutAndFieldMatchesTheCCompiler() throws Exception {
        NativeLibrary.load();
        Path sources = Path.of("src/main/java/io/krkafka/ffi");
        // Share the native golden's complete public field inventory, while C
        // sizeof/offsetof and jextract's Java layouts calculate values independently.
        var inventories = new java.util.HashMap<String, java.util.Set<String>>();
        var inventory = java.util.regex.Pattern.compile("layout!\\(\\s*\\w+\\s*,\\s*\"([^\"]+)\"\\s*,\\s*(.*?)\\);", java.util.regex.Pattern.DOTALL)
            .matcher(Files.readString(Path.of("../kr-kafka-ffi/tests/c_header.rs")));
        while (inventory.find()) inventories.put(inventory.group(1), java.util.Arrays.stream(inventory.group(2).split(","))
            .map(String::strip).filter(s -> !s.isEmpty()).collect(java.util.stream.Collectors.toSet()));
        var actualTypes = new java.util.HashSet<String>();
        List<String> rows = new ArrayList<>();
        StringBuilder c = new StringBuilder("#include <stdio.h>\n#include \"kr_kafka.h\"\nint main(void) {\n");
        try (var paths = Files.list(sources)) {
            for (Path source : paths.sorted().toList()) {
                String name = source.getFileName().toString().replace(".java", "");
                if (name.startsWith("kr_kafka_h")) continue;
                Class<?> type = Class.forName("io.krkafka.ffi." + name);
                actualTypes.add(name);
                var actualFields = new java.util.HashSet<String>();
                MemoryLayout layout = (MemoryLayout) type.getMethod("layout").invoke(null);
                rows.add(name + " size " + layout.byteSize());
                rows.add(name + " align " + layout.byteAlignment());
                c.append("printf(\"").append(name).append(" size %zu\\n\", sizeof(").append(name).append("));\n");
                c.append("printf(\"").append(name).append(" align %zu\\n\", _Alignof(").append(name).append("));\n");
                for (var method : java.util.Arrays.stream(type.getDeclaredMethods()).sorted(java.util.Comparator.comparing(java.lang.reflect.Method::getName)).toList()) {
                    if (!method.getName().endsWith("$offset")) continue;
                    String field = method.getName().replace("$offset", "");
                    actualFields.add(field);
                    rows.add(name + " " + field + " " + method.invoke(null));
                    c.append("printf(\"").append(name).append(" ").append(field).append(" %zu\\n\", offsetof(")
                        .append(name).append(", ").append(field).append("));\n");
                }
                assertEquals(inventories.get(name), actualFields, "native golden field inventory for " + name);
            }
        }
        c.append("return 0; }\n");
        assertEquals(inventories.keySet(), actualTypes, "complete native golden type inventory");
        Path directory = Files.createTempDirectory("kr-java-layout-");
        Path source = directory.resolve("layout.c"), binary = directory.resolve("layout");
        try {
            Files.writeString(source, c);
            var compiler = new ProcessBuilder("cc", "-std=c11", "-Werror", "-I../kr-kafka-ffi/include", source.toString(), "-o", binary.toString()).redirectErrorStream(true).start();
            String diagnostics = new String(compiler.getInputStream().readAllBytes());
            assertEquals(0, compiler.waitFor(), diagnostics);
            var process = new ProcessBuilder(binary.toString()).start();
            String output = new String(process.getInputStream().readAllBytes());
            assertEquals(0, process.waitFor());
            assertEquals(rows, output.lines().toList());
        } finally {
            Files.deleteIfExists(binary); Files.deleteIfExists(source); Files.deleteIfExists(directory);
        }
    }
}

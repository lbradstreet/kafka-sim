/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.dst.compat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** Test-only Panama downcalls into the shared Rust simulation environment. */
final class ScenarioBridge implements AutoCloseable {
    static final ObjectMapper JSON = new ObjectMapper();
    private final Arena libraryArena = Arena.ofConfined();
    private final MethodHandle call;
    private final MethodHandle length;

    ScenarioBridge(Path library) {
        SymbolLookup symbols = SymbolLookup.libraryLookup(library, libraryArena);
        Linker linker = Linker.nativeLinker();
        call = linker.downcallHandle(symbols.find("kr_sim_call").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
        length = linker.downcallHandle(symbols.find("kr_sim_reply_len").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG));
    }

    JsonNode call(String operation, Object... fields) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("op", operation);
        for (int i = 0; i < fields.length; i += 2)
            request.put((String) fields[i], fields[i + 1]);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment input = arena.allocateFrom(JSON.writeValueAsString(request));
            MemorySegment output = (MemorySegment) call.invokeExact(input);
            long size = (long) length.invokeExact();
            if (size < 1 || size > 512L * 1024 * 1024)
                throw new IllegalStateException("Native reply bounds: " + size);
            JsonNode reply = JSON.readTree(output.reinterpret(size).getString(0));
            if (reply.has("error"))
                throw new IllegalStateException(reply.get("error").asText());
            return reply.get("ok");
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalStateException("Simulation bridge " + operation, e);
        }
    }

    static byte[] bytes(JsonNode array) {
        if (array.isNull())
            return null;
        byte[] result = new byte[array.size()];
        for (int i = 0; i < result.length; i++)
            result[i] = (byte) array.get(i).intValue();
        return result;
    }

    @Override
    public void close() {
        call("destroy");
        libraryArena.close();
    }
}

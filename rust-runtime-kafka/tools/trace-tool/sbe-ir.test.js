import "./sbe-ir.js";
import "./dst-trace-sbe-ir.js";
import "./testdata/generic-sbe-ir.js";

const SBE = globalThis.SbeIr;
const DST_GOLDEN = await Deno.readFile(
  new URL("./testdata/browser-sbe-c1-s1v1-a8-t5.sbe", import.meta.url),
);

function fail(message) {
  throw new Error(message);
}

function assert(condition, message) {
  if (!condition) fail(message);
}

function assertEquals(actual, expected, message) {
  const normalize = (value) => {
    if (typeof value === "bigint") return `${value}n`;
    if (Array.isArray(value)) return value.map(normalize);
    if (value !== null && typeof value === "object") {
      return Object.fromEntries(
        Object.entries(value).map(([key, entry]) => [key, normalize(entry)]),
      );
    }
    return value;
  };
  const actualText = JSON.stringify(normalize(actual));
  const expectedText = JSON.stringify(normalize(expected));
  if (actualText !== expectedText) {
    fail(`${message}\nexpected: ${expectedText}\nactual:   ${actualText}`);
  }
}

function expectError(action, type, fragment, message) {
  let thrown = null;
  try {
    action();
  } catch (error) {
    thrown = error;
  }
  assert(
    thrown instanceof type,
    `${message}: expected ${type.name}, got ${thrown}`,
  );
  assert(
    thrown.message.includes(fragment),
    `${message}: expected ${JSON.stringify(fragment)} in ${
      JSON.stringify(thrown.message)
    }`,
  );
}

function irBytes(module) {
  const bytes = SBE.decodeBase64(module.base64);
  assert(
    bytes.length === module.byteLength,
    "generated IR byte length differs",
  );
  return bytes;
}

function serializedIrTokens(bytes) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = 12;
  for (let field = 0; field < 3; field += 1) {
    const length = view.getUint16(offset, true);
    offset += 2 + length;
  }
  const tokens = [];
  while (offset < bytes.length) {
    const base = offset;
    const signal = view.getUint8(base + 20);
    offset += 28;
    const fields = [];
    for (let field = 0; field < 12; field += 1) {
      const length = view.getUint16(offset, true);
      const start = offset + 2;
      fields.push({ start, length });
      offset = start + length;
    }
    const name = new TextDecoder().decode(
      bytes.subarray(fields[0].start, fields[0].start + fields[0].length),
    );
    tokens.push({ base, signal, name, fields });
  }
  return tokens;
}

async function sha256(bytes) {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}

function frameEnd(bytes, frameOffset) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return frameOffset + view.getUint32(frameOffset, true);
}

function concat(parts) {
  const length = parts.reduce((sum, part) => sum + part.length, 0);
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    bytes.set(part, offset);
    offset += part.length;
  }
  return bytes;
}

const UTF8 = new TextEncoder();

function sizedUtf8(value) {
  const data = UTF8.encode(value);
  const bytes = new Uint8Array(2 + data.length);
  new DataView(bytes.buffer).setUint16(0, data.length, false);
  bytes.set(data, 2);
  return bytes;
}

function sizedRaw(data) {
  const bytes = new Uint8Array(2 + data.length);
  new DataView(bytes.buffer).setUint16(0, data.length, false);
  bytes.set(data, 2);
  return bytes;
}

function groupEntry(price, quantity, side, note) {
  const fixed = new Uint8Array(13);
  const view = new DataView(fixed.buffer);
  view.setInt32(0, price, false);
  view.setBigUint64(4, quantity, false);
  view.setUint8(12, side);
  return concat([fixed, sizedUtf8(note)]);
}

function genericMessage(
  {
    actingVersion = 2,
    blockLength = actingVersion >= 2 ? 27 : actingVersion >= 1 ? 26 : 18,
  } = {},
) {
  const headerAndFixed = new Uint8Array(8 + blockLength);
  const view = new DataView(headerAndFixed.buffer);
  view.setUint16(0, blockLength, false);
  view.setUint16(2, 7, false);
  view.setUint16(4, 42, false);
  view.setUint16(6, actingVersion, false);
  view.setBigUint64(8, 18_446_744_073_709_551_615n, false);
  view.setUint8(16, "A".charCodeAt(0));
  view.setInt16(20, -123, false);
  view.setInt16(22, 456, false);
  view.setUint8(24, 2);
  view.setUint8(25, 5);
  if (blockLength >= 26) {
    view.setBigUint64(26, 18_364_758_544_493_064_705n, false);
  }
  if (blockLength >= 27) view.setUint8(34, "Z".charCodeAt(0));

  const dimensions = new Uint8Array(4);
  const dimensionView = new DataView(dimensions.buffer);
  dimensionView.setUint16(0, 13, false);
  dimensionView.setUint16(2, 2, false);

  return concat([
    headerAndFixed,
    dimensions,
    groupEntry(-20, 18_446_744_073_709_551_615n, 1, "α"),
    groupEntry(300, 42n, 2, "\u{feff}note"),
    sizedUtf8("雪"),
    sizedRaw(Uint8Array.of(0, 0xff, 7)),
  ]);
}

Deno.test("generated IR modules carry exact tool and checksum metadata", async () => {
  for (
    const module of [globalThis.DstTraceSbeIr, globalThis.GenericSbeFixtureIr]
  ) {
    const bytes = irBytes(module);
    assert(
      module.sbeToolVersion === "1.38.1",
      "generated module tool version differs",
    );
    assert(
      await sha256(bytes) === module.sha256,
      "generated module checksum differs",
    );
  }
});

Deno.test("official DST IR parses and decodes generated messages exactly", () => {
  const schema = SBE.parse(irBytes(globalThis.DstTraceSbeIr));
  assertEquals(
    {
      id: schema.id,
      version: schema.version,
      semanticVersion: schema.semanticVersion,
      headerLength: schema.headerLength,
      messageCount: schema.messages.length,
      first: schema.messages[0],
      last: schema.messages.at(-1),
    },
    {
      id: 1,
      version: 1,
      semanticVersion: "2.0.0",
      headerLength: 8,
      messageCount: 23,
      first: {
        id: 2,
        name: "RandomStreamState",
        blockLength: 24,
        version: 0,
      },
      last: {
        id: 120,
        name: "TaskSpawned",
        blockLength: 33,
        version: 0,
      },
    },
    "DST schema summary differs",
  );
  assert(schema.message(107)?.name === "TaskPanicked", "message lookup failed");
  assert(schema.message(999) === null, "unknown message lookup must be null");

  const offset = 20;
  const end = frameEnd(DST_GOLDEN, 16);
  const decodedHeader = SBE.decodeHeader(schema, DST_GOLDEN, { offset, end });
  assertEquals(
    decodedHeader.header,
    { blockLength: 240, templateId: 4, schemaId: 1, version: 1 },
    "schema-driven header differs",
  );
  const decoded = SBE.decodeMessage(schema, DST_GOLDEN, { offset, end });
  assert(
    decoded.endOffset === end,
    "artifact header did not consume its frame",
  );
  assert(decoded.bytesRead === end - offset, "artifact byte count differs");
  assert(
    decoded.template === schema.message(4),
    "decoder did not return the public template",
  );
  assert(
    decoded.value.seed === 18_446_744_073_709_551_615n,
    "uint64 precision was lost",
  );
  assert(decoded.value.driver.startsWith("\u{feff}"), "UTF-8 BOM was stripped");
  assert(decoded.value.taskCount === 3, "uint32 field differs");
});

Deno.test("generic decoder handles big-endian composites enums sets groups data constants and evolution", () => {
  const schema = SBE.parse(irBytes(globalThis.GenericSbeFixtureIr));
  assertEquals(
    {
      id: schema.id,
      version: schema.version,
      headerLength: schema.headerLength,
      messages: schema.messages,
    },
    {
      id: 42,
      version: 2,
      headerLength: 8,
      messages: [{ id: 7, name: "Snapshot", blockLength: 27, version: 0 }],
    },
    "generic fixture schema differs",
  );

  const bytes = genericMessage();
  const decoded = SBE.decodeMessage(schema, bytes);
  assert(
    decoded.endOffset === bytes.length,
    "generic fixture did not consume its message",
  );
  assertEquals(
    decoded.value,
    {
      sequence: 18_446_744_073_709_551_615n,
      symbol: "A",
      point: { x: -123, y: 456 },
      side: { name: "Sell", value: 2 },
      flags: { choices: ["urgent", "replay"], value: 5 },
      optionalCounter: 18_364_758_544_493_064_705n,
      venue: 513,
      market: "XNAS",
      optionalCode: "Z",
      constantSide: { name: "Buy", value: 1 },
      entries: [
        {
          price: -20,
          quantity: 18_446_744_073_709_551_615n,
          side: { name: "Buy", value: 1 },
          note: "α",
        },
        {
          price: 300,
          quantity: 42n,
          side: { name: "Sell", value: 2 },
          note: "\u{feff}note",
        },
      ],
      memo: "雪",
      blob: Uint8Array.of(0, 0xff, 7),
    },
    "generic decoded value differs",
  );
  assert(
    decoded.value.blob instanceof Uint8Array,
    "raw data is not byte typed",
  );
  decoded.value.blob[0] = 99;
  assert(
    SBE.decodeMessage(schema, bytes).value.blob[0] === 0,
    "raw data aliases the message input",
  );

  const oldBytes = genericMessage({ actingVersion: 0 });
  const old = SBE.decodeMessage(schema, oldBytes);
  assert(old.header.blockLength === 18, "old acting block length differs");
  assert(
    old.value.optionalCounter === null,
    "future optional field should be absent",
  );
  assert(old.value.venue === 513, "constant field should remain available");
  assert(
    old.value.market === "XNAS",
    "constant character field should remain available",
  );
  assert(
    old.value.optionalCode === null,
    "future optional character field should be absent",
  );
  assert(old.value.entries.length === 2, "old message group differs");

  const nullOptional = bytes.slice();
  nullOptional[34] = 0;
  assert(
    SBE.decodeMessage(schema, nullOptional).value.optionalCode === null,
    "optional character null value was not recognized",
  );
});

Deno.test("generic decoder enforces message, group, var-data, and exact-length bounds", () => {
  const schema = SBE.parse(irBytes(globalThis.GenericSbeFixtureIr));
  const bytes = genericMessage();
  const shortBlock = bytes.slice();
  new DataView(shortBlock.buffer).setUint16(0, 0, false);
  expectError(
    () => SBE.decodeMessage(schema, shortBlock),
    SBE.SbeDecodeError,
    "does not fit the acting block length",
    "required fixed field",
  );
  expectError(
    () =>
      SBE.decodeMessage(schema, bytes, {
        limits: { maxMessageBytes: bytes.length - 1 },
      }),
    SBE.SbeDecodeError,
    "message exceeds",
    "message bound",
  );
  expectError(
    () => SBE.decodeMessage(schema, bytes, { limits: { maxGroupEntries: 1 } }),
    SBE.SbeDecodeError,
    "Snapshot.entries count exceeds 1",
    "group bound",
  );
  expectError(
    () => SBE.decodeMessage(schema, bytes, { limits: { maxVarDataBytes: 1 } }),
    SBE.SbeDecodeError,
    "Snapshot.entries[0].note length exceeds 1",
    "var-data bound",
  );

  const trailing = new Uint8Array(bytes.length + 1);
  trailing.set(bytes);
  expectError(
    () => SBE.decodeMessage(schema, trailing),
    SBE.SbeDecodeError,
    "trailing bytes",
    "exact length",
  );
  const accepted = SBE.decodeMessage(schema, trailing, {
    requireExactLength: false,
  });
  assert(
    accepted.endOffset === bytes.length,
    "non-exact decode consumed trailing data",
  );
});

Deno.test("generic decoder rejects invalid coordinates, UTF-8, and truncation", () => {
  const schema = SBE.parse(irBytes(globalThis.GenericSbeFixtureIr));
  const bytes = genericMessage();
  for (const end of [0, 1, 7, 8, 20, bytes.length - 1]) {
    expectError(
      () => SBE.decodeMessage(schema, bytes.subarray(0, end)),
      SBE.SbeDecodeError,
      "truncated",
      `message truncation ${end}`,
    );
  }

  const wrongSchema = bytes.slice();
  new DataView(wrongSchema.buffer).setUint16(4, 43, false);
  expectError(
    () => SBE.decodeMessage(schema, wrongSchema),
    SBE.SbeDecodeError,
    "schema ID 43 does not match 42",
    "schema ID",
  );

  const unknownTemplate = bytes.slice();
  new DataView(unknownTemplate.buffer).setUint16(2, 8, false);
  expectError(
    () => SBE.decodeMessage(schema, unknownTemplate),
    SBE.SbeDecodeError,
    "unknown template ID 8",
    "template ID",
  );

  const invalidUtf8 = bytes.slice();
  const secondNote = 8 + 27 + 4 + 13 + 2 + UTF8.encode("α").length + 13 + 2;
  invalidUtf8[secondNote] = 0xff;
  expectError(
    () => SBE.decodeMessage(schema, invalidUtf8),
    SBE.SbeDecodeError,
    "not valid UTF-8",
    "UTF-8",
  );
});

Deno.test("IR and base64 parsers reject malformed and bounded inputs", () => {
  const bytes = irBytes(globalThis.GenericSbeFixtureIr);
  for (const end of [0, 1, 11, 12, bytes.length - 1]) {
    expectError(
      () => SBE.parse(bytes.subarray(0, end)),
      SBE.SbeIrError,
      "truncated",
      `IR truncation ${end}`,
    );
  }

  const version = bytes.slice();
  new DataView(version.buffer).setInt32(4, 1, true);
  expectError(
    () => SBE.parse(version),
    SBE.SbeIrError,
    "unsupported IR version 1",
    "IR version",
  );
  expectError(
    () => SBE.parse(bytes, { limits: { maxIrBytes: bytes.length - 1 } }),
    SBE.SbeIrError,
    "file exceeds",
    "IR byte bound",
  );
  expectError(
    () => SBE.parse(bytes, { limits: { maxTokens: 1 } }),
    SBE.SbeIrError,
    "token count exceeds 1",
    "IR token bound",
  );
  expectError(
    () => SBE.decodeBase64("%%%="),
    SBE.SbeIrError,
    "malformed base64",
    "base64 syntax",
  );
  expectError(
    () =>
      SBE.decodeBase64(globalThis.GenericSbeFixtureIr.base64, {
        maxBytes: bytes.length - 1,
      }),
    SBE.SbeIrError,
    "base64 payload exceeds",
    "base64 bound",
  );

  const tokens = serializedIrTokens(bytes);
  const wrongHeader = bytes.slice();
  new DataView(wrongHeader.buffer).setUint8(tokens[0].base + 20, 1);
  expectError(
    () => SBE.parse(wrongHeader),
    SBE.SbeIrError,
    "wrong begin signal",
    "header begin signal",
  );

  const snapshot = tokens.find((token) =>
    token.signal === 1 && token.name === "Snapshot"
  );
  assert(snapshot !== undefined, "serialized IR has no Snapshot token");
  const crossedMessage = bytes.slice();
  const crossedView = new DataView(crossedMessage.buffer);
  crossedView.setInt32(
    snapshot.base + 16,
    crossedView.getInt32(snapshot.base + 16, true) - 1,
    true,
  );
  expectError(
    () => SBE.parse(crossedMessage),
    SBE.SbeIrError,
    "matching end token",
    "message component boundary",
  );

  const replay = tokens.find((token) =>
    token.signal === 13 && token.name === "replay"
  );
  assert(replay?.fields[1].length === 1, "serialized IR replay choice differs");
  const wideChoice = bytes.slice();
  wideChoice[replay.fields[1].start] = 8;
  expectError(
    () => SBE.parse(wideChoice),
    SBE.SbeIrError,
    "exceeds its encoding width",
    "set choice width",
  );
});

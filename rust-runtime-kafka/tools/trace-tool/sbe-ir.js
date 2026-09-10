(function (root) {
  "use strict";

  const IR_VERSION = 0;
  const FRAME_FIXED_LENGTH = 12;
  const TOKEN_FIXED_LENGTH = 28;
  const TOKEN_VARIABLE_FIELDS = 12;
  const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);

  const SIGNAL = Object.freeze({
    BEGIN_MESSAGE: 1,
    END_MESSAGE: 2,
    BEGIN_COMPOSITE: 3,
    END_COMPOSITE: 4,
    BEGIN_FIELD: 5,
    END_FIELD: 6,
    BEGIN_GROUP: 7,
    END_GROUP: 8,
    BEGIN_ENUM: 9,
    VALID_VALUE: 10,
    END_ENUM: 11,
    BEGIN_SET: 12,
    CHOICE: 13,
    END_SET: 14,
    BEGIN_VAR_DATA: 15,
    END_VAR_DATA: 16,
    ENCODING: 17,
  });

  const PRIMITIVE = Object.freeze({
    NONE: 0,
    CHAR: 1,
    INT8: 2,
    INT16: 3,
    INT32: 4,
    INT64: 5,
    UINT8: 6,
    UINT16: 7,
    UINT32: 8,
    UINT64: 9,
    FLOAT: 10,
    DOUBLE: 11,
  });

  const PRIMITIVE_WIDTH = Object.freeze([0, 1, 1, 2, 4, 8, 1, 2, 4, 8, 4, 8]);
  const DEFAULT_LIMITS = Object.freeze({
    maxIrBytes: 8 * 1024 * 1024,
    maxTokens: 100_000,
    maxTotalTokenDataBytes: 8 * 1024 * 1024,
    maxMessages: 10_000,
    maxNestingDepth: 32,
    maxMessageBytes: 64 * 1024 * 1024,
    maxGroupEntries: 1_000_000,
    maxTotalGroupEntries: 1_000_000,
    maxVarDataBytes: 16 * 1024 * 1024,
  });
  const UTF8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  const INTERNAL = new WeakMap();

  class SbeIrError extends Error {
    constructor(message) {
      super(`Invalid SBE IR: ${message}`);
      this.name = "SbeIrError";
    }
  }

  class SbeDecodeError extends Error {
    constructor(message) {
      super(`Invalid SBE message: ${message}`);
      this.name = "SbeDecodeError";
    }
  }

  function irFail(message) {
    throw new SbeIrError(message);
  }

  function decodeFail(message) {
    throw new SbeDecodeError(message);
  }

  function asBytes(input, description) {
    if (input instanceof Uint8Array) {
      return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    }
    if (ArrayBuffer.isView(input)) {
      return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    }
    if (input instanceof ArrayBuffer) return new Uint8Array(input);
    throw new TypeError(
      `${description} must be an ArrayBuffer or typed-array view`,
    );
  }

  function checkedLimits(overrides = null) {
    if (overrides === null || overrides === undefined) return DEFAULT_LIMITS;
    if (typeof overrides !== "object" || Array.isArray(overrides)) {
      throw new TypeError("SBE limits must be an object");
    }
    const limits = { ...DEFAULT_LIMITS };
    for (const [name, value] of Object.entries(overrides)) {
      if (!(name in limits)) throw new TypeError(`unknown SBE limit ${name}`);
      if (!Number.isSafeInteger(value) || value <= 0) {
        throw new TypeError(
          `SBE limit ${name} must be a positive safe integer`,
        );
      }
      limits[name] = value;
    }
    return Object.freeze(limits);
  }

  class Reader {
    constructor(bytes, end = bytes.length, fail = irFail) {
      this.bytes = bytes;
      this.view = new DataView(
        bytes.buffer,
        bytes.byteOffset,
        bytes.byteLength,
      );
      this.offset = 0;
      this.end = end;
      this.fail = fail;
    }

    require(offset, length, description) {
      if (
        !Number.isSafeInteger(offset) || !Number.isSafeInteger(length) ||
        offset < 0 || length < 0 || offset > this.end - length
      ) {
        this.fail(`truncated ${description}`);
      }
    }

    i32(description) {
      this.require(this.offset, 4, description);
      const value = this.view.getInt32(this.offset, true);
      this.offset += 4;
      return value;
    }

    u8(description) {
      this.require(this.offset, 1, description);
      return this.view.getUint8(this.offset++);
    }

    u16(description) {
      this.require(this.offset, 2, description);
      const value = this.view.getUint16(this.offset, true);
      this.offset += 2;
      return value;
    }

    variableBytes(description, budget) {
      const length = this.u16(`${description} length`);
      this.require(this.offset, length, description);
      budget.total += length;
      if (budget.total > budget.maximum) {
        this.fail("token variable data exceeds its cumulative bound");
      }
      const value = this.bytes.slice(this.offset, this.offset + length);
      this.offset += length;
      return value;
    }
  }

  function utf8(bytes, description, fail = irFail) {
    try {
      return UTF8.decode(bytes);
    } catch (_error) {
      fail(`${description} is not valid UTF-8`);
    }
  }

  function hex(bytes) {
    let value = "";
    for (const byte of bytes) value += byte.toString(16).padStart(2, "0");
    return value;
  }

  function bytesFromHex(value) {
    if (value.length % 2 !== 0) {
      irFail("internal primitive-value hex has odd length");
    }
    const bytes = new Uint8Array(value.length / 2);
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
    }
    return bytes;
  }

  function decodeBase64(source, options = {}) {
    if (typeof source !== "string") {
      throw new TypeError("base64 input must be a string");
    }
    const maximum = options.maxBytes ?? DEFAULT_LIMITS.maxIrBytes;
    if (!Number.isSafeInteger(maximum) || maximum <= 0) {
      throw new TypeError("base64 maxBytes must be a positive safe integer");
    }
    if (
      source.length % 4 !== 0 ||
      !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(
        source,
      )
    ) {
      throw new SbeIrError("malformed base64");
    }
    const padding = source.endsWith("==") ? 2 : source.endsWith("=") ? 1 : 0;
    const decodedLength = source.length / 4 * 3 - padding;
    if (decodedLength > maximum) {
      throw new SbeIrError(`base64 payload exceeds the ${maximum}-byte bound`);
    }
    let binary;
    try {
      binary = atob(source);
    } catch (_error) {
      throw new SbeIrError("malformed base64");
    }
    if (binary.length !== decodedLength) {
      throw new SbeIrError("malformed base64");
    }
    const bytes = new Uint8Array(decodedLength);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return bytes;
  }

  function parseToken(reader, index, budget) {
    reader.require(
      reader.offset,
      TOKEN_FIXED_LENGTH,
      `token ${index} fixed block`,
    );
    const tokenOffset = reader.i32(`token ${index} offset`);
    const encodedLength = reader.i32(`token ${index} encoded length`);
    const fieldId = reader.i32(`token ${index} field ID`);
    const version = reader.i32(`token ${index} version`);
    const componentTokenCount = reader.i32(`token ${index} component count`);
    const signal = reader.u8(`token ${index} signal`);
    const primitiveType = reader.u8(`token ${index} primitive type`);
    const byteOrder = reader.u8(`token ${index} byte order`);
    const presence = reader.u8(`token ${index} presence`);
    const deprecated = reader.i32(`token ${index} deprecated version`);
    const raw = Array.from(
      { length: TOKEN_VARIABLE_FIELDS },
      (_unused, field) =>
        reader.variableBytes(`token ${index} variable field ${field}`, budget),
    );

    if (tokenOffset < -1) {
      irFail(`token ${index} has invalid offset ${tokenOffset}`);
    }
    if (encodedLength < -1) {
      irFail(`token ${index} has invalid encoded length ${encodedLength}`);
    }
    if (fieldId < -1) irFail(`token ${index} has invalid field ID ${fieldId}`);
    if (version < 0) irFail(`token ${index} has negative version`);
    if (componentTokenCount <= 0) {
      irFail(`token ${index} has a nonpositive component count`);
    }
    if (signal < SIGNAL.BEGIN_MESSAGE || signal > SIGNAL.ENCODING) {
      irFail(`token ${index} has unknown signal ${signal}`);
    }
    if (primitiveType < PRIMITIVE.NONE || primitiveType > PRIMITIVE.DOUBLE) {
      irFail(`token ${index} has unknown primitive type ${primitiveType}`);
    }
    if (byteOrder > 1) {
      irFail(`token ${index} has unknown byte order ${byteOrder}`);
    }
    if (presence > 2) irFail(`token ${index} has unknown presence ${presence}`);
    if (deprecated < 0) {
      irFail(`token ${index} has negative deprecated version`);
    }

    const text = (field, description) =>
      utf8(raw[field], `token ${index} ${description}`);
    return Object.freeze({
      index,
      offset: tokenOffset,
      encodedLength,
      fieldId,
      version,
      componentTokenCount,
      signal,
      primitiveType,
      byteOrder,
      presence,
      deprecated,
      name: text(0, "name"),
      constValue: hex(raw[1]),
      minValue: hex(raw[2]),
      maxValue: hex(raw[3]),
      nullValue: hex(raw[4]),
      characterEncoding: text(5, "character encoding"),
      epoch: text(6, "epoch"),
      timeUnit: text(7, "time unit"),
      semanticType: text(8, "semantic type"),
      description: text(9, "description"),
      referencedName: text(10, "referenced name"),
      packageName: text(11, "package name"),
    });
  }

  function component(tokens, index, beginSignal, endSignal, description) {
    const begin = tokens[index];
    if (begin === undefined || begin.signal !== beginSignal) {
      irFail(`${description} at token ${index} has the wrong begin signal`);
    }
    const endIndex = index + begin.componentTokenCount - 1;
    if (endIndex <= index || endIndex >= tokens.length) {
      irFail(`${description} at token ${index} exceeds the token stream`);
    }
    const end = tokens[endIndex];
    if (end.signal !== endSignal || end.name !== begin.name) {
      irFail(`${description} at token ${index} has no matching end token`);
    }
    if (end.componentTokenCount !== begin.componentTokenCount) {
      irFail(
        `${description} at token ${index} has inconsistent component counts`,
      );
    }
    return { begin, endIndex };
  }

  function primitiveWidth(type, description) {
    const width = PRIMITIVE_WIDTH[type];
    if (width === undefined || width === 0) {
      irFail(`${description} has no primitive encoding`);
    }
    return width;
  }

  function requirePrimitiveBytes(value, width, description) {
    if (value.length !== width * 2) {
      irFail(`${description} does not contain exactly one primitive value`);
    }
  }

  function compileEncoding(token) {
    if (token.signal !== SIGNAL.ENCODING || token.componentTokenCount !== 1) {
      irFail(`token ${token.index} is not a scalar encoding token`);
    }
    const width = primitiveWidth(token.primitiveType, `encoding ${token.name}`);
    if (token.encodedLength < 0) {
      return Object.freeze({
        kind: "encoding",
        name: token.name,
        offset: token.offset,
        encodedLength: token.encodedLength,
        primitiveType: token.primitiveType,
        byteOrder: token.byteOrder,
        presence: token.presence,
        version: token.version,
        characterEncoding: token.characterEncoding,
        constValue: token.constValue,
        nullValue: token.nullValue,
        width,
        arrayLength: -1,
      });
    }
    if (token.presence !== 2 && token.encodedLength % width !== 0) {
      irFail(
        `encoding ${token.name} length is not a multiple of its primitive width`,
      );
    }
    if (
      token.presence === 2 &&
      (token.constValue.length === 0 ||
        token.constValue.length % (width * 2) !== 0)
    ) {
      irFail(`constant encoding ${token.name} has an invalid value length`);
    }
    if (token.nullValue && token.nullValue.length !== width * 2) {
      irFail(`encoding ${token.name} has an invalid null value length`);
    }
    const arrayLength = token.presence === 2
      ? Math.max(1, token.constValue.length / (width * 2))
      : token.encodedLength / width;
    return Object.freeze({
      kind: "encoding",
      name: token.name,
      offset: token.offset,
      encodedLength: token.encodedLength,
      primitiveType: token.primitiveType,
      byteOrder: token.byteOrder,
      presence: token.presence,
      version: token.version,
      characterEncoding: token.characterEncoding,
      constValue: token.constValue,
      nullValue: token.nullValue,
      width,
      arrayLength,
    });
  }

  function compileEnum(tokens, index, depth, limits) {
    if (depth > limits.maxNestingDepth) {
      irFail("schema nesting exceeds its bound");
    }
    const { begin, endIndex } = component(
      tokens,
      index,
      SIGNAL.BEGIN_ENUM,
      SIGNAL.END_ENUM,
      "enum",
    );
    const width = primitiveWidth(begin.primitiveType, `enum ${begin.name}`);
    if ([PRIMITIVE.FLOAT, PRIMITIVE.DOUBLE].includes(begin.primitiveType)) {
      irFail(`enum ${begin.name} is not integer encoded`);
    }
    if (begin.presence === 2) {
      requirePrimitiveBytes(
        begin.constValue,
        width,
        `enum ${begin.name} constant`,
      );
    }
    const values = [];
    const names = new Set();
    for (let cursor = index + 1; cursor < endIndex; cursor += 1) {
      const token = tokens[cursor];
      if (
        token.signal !== SIGNAL.VALID_VALUE || token.componentTokenCount !== 1
      ) {
        irFail(`enum ${begin.name} contains a non-value token at ${cursor}`);
      }
      if (!token.name || names.has(token.name)) {
        irFail(`enum ${begin.name} has a duplicate or empty value name`);
      }
      requirePrimitiveBytes(
        token.constValue,
        width,
        `enum ${begin.name} value ${token.name}`,
      );
      names.add(token.name);
      values.push(
        Object.freeze({ name: token.name, constValue: token.constValue }),
      );
    }
    return {
      plan: Object.freeze({
        kind: "enum",
        name: begin.name,
        offset: begin.offset,
        encodedLength: begin.encodedLength,
        primitiveType: begin.primitiveType,
        byteOrder: begin.byteOrder,
        presence: begin.presence,
        version: begin.version,
        constValue: begin.constValue,
        nullValue: begin.nullValue,
        values: Object.freeze(values),
      }),
      next: endIndex + 1,
    };
  }

  function compileSet(tokens, index, depth, limits) {
    if (depth > limits.maxNestingDepth) {
      irFail("schema nesting exceeds its bound");
    }
    const { begin, endIndex } = component(
      tokens,
      index,
      SIGNAL.BEGIN_SET,
      SIGNAL.END_SET,
      "set",
    );
    const width = primitiveWidth(begin.primitiveType, `set ${begin.name}`);
    if (
      ![
        PRIMITIVE.UINT8,
        PRIMITIVE.UINT16,
        PRIMITIVE.UINT32,
        PRIMITIVE.UINT64,
      ].includes(begin.primitiveType)
    ) {
      irFail(`set ${begin.name} is not unsigned-integer encoded`);
    }
    if (begin.presence === 2) {
      requirePrimitiveBytes(
        begin.constValue,
        width,
        `set ${begin.name} constant`,
      );
    }
    const choices = [];
    const names = new Set();
    for (let cursor = index + 1; cursor < endIndex; cursor += 1) {
      const token = tokens[cursor];
      if (token.signal !== SIGNAL.CHOICE || token.componentTokenCount !== 1) {
        irFail(`set ${begin.name} contains a non-choice token at ${cursor}`);
      }
      if (!token.name || names.has(token.name)) {
        irFail(`set ${begin.name} has a duplicate or empty choice name`);
      }
      requirePrimitiveBytes(
        token.constValue,
        width,
        `set ${begin.name} choice ${token.name}`,
      );
      const bit = primitiveFromHex(
        token.constValue,
        begin.primitiveType,
        begin.byteOrder,
        `set ${begin.name} choice ${token.name}`,
      );
      if (BigInt(bit) >= BigInt(width * 8)) {
        irFail(
          `set ${begin.name} choice ${token.name} exceeds its encoding width`,
        );
      }
      names.add(token.name);
      choices.push(
        Object.freeze({ name: token.name, constValue: token.constValue }),
      );
    }
    return {
      plan: Object.freeze({
        kind: "set",
        name: begin.name,
        offset: begin.offset,
        encodedLength: begin.encodedLength,
        primitiveType: begin.primitiveType,
        byteOrder: begin.byteOrder,
        presence: begin.presence,
        version: begin.version,
        constValue: begin.constValue,
        nullValue: begin.nullValue,
        choices: Object.freeze(choices),
      }),
      next: endIndex + 1,
    };
  }

  function compileType(tokens, index, depth, limits) {
    const token = tokens[index];
    if (token === undefined) irFail("component ends before its type token");
    switch (token.signal) {
      case SIGNAL.ENCODING:
        return { plan: compileEncoding(token), next: index + 1 };
      case SIGNAL.BEGIN_COMPOSITE:
        return compileComposite(tokens, index, depth + 1, limits);
      case SIGNAL.BEGIN_ENUM:
        return compileEnum(tokens, index, depth + 1, limits);
      case SIGNAL.BEGIN_SET:
        return compileSet(tokens, index, depth + 1, limits);
      default:
        irFail(`token ${index} cannot begin a field type`);
    }
  }

  function compileComposite(tokens, index, depth, limits) {
    if (depth > limits.maxNestingDepth) {
      irFail("schema nesting exceeds its bound");
    }
    const { begin, endIndex } = component(
      tokens,
      index,
      SIGNAL.BEGIN_COMPOSITE,
      SIGNAL.END_COMPOSITE,
      "composite",
    );
    if (!begin.name) {
      irFail(`composite token ${index} has an empty name`);
    }
    const members = [];
    const names = new Set();
    let cursor = index + 1;
    while (cursor < endIndex) {
      const compiled = compileType(tokens, cursor, depth, limits);
      const member = compiled.plan;
      if (!member.name || names.has(member.name)) {
        irFail(`composite ${begin.name} has a duplicate or empty member name`);
      }
      names.add(member.name);
      members.push(member);
      cursor = compiled.next;
    }
    if (cursor !== endIndex) {
      irFail(`composite ${begin.name} crosses its component boundary`);
    }
    return {
      plan: Object.freeze({
        kind: "composite",
        name: begin.name,
        offset: begin.offset,
        encodedLength: begin.encodedLength,
        presence: begin.presence,
        version: begin.version,
        members: Object.freeze(members),
      }),
      next: endIndex + 1,
    };
  }

  function compileField(tokens, index, depth, limits) {
    const { begin, endIndex } = component(
      tokens,
      index,
      SIGNAL.BEGIN_FIELD,
      SIGNAL.END_FIELD,
      "field",
    );
    if (
      !begin.name || begin.fieldId < 0 || begin.offset < 0 ||
      begin.encodedLength < 0
    ) {
      irFail(`field token ${index} has invalid identity or offset`);
    }
    const compiled = compileType(tokens, index + 1, depth, limits);
    if (compiled.next !== endIndex) {
      irFail(`field ${begin.name} contains extra type tokens`);
    }
    let type = compiled.plan;
    if (type.kind === "composite" && type.encodedLength < 0) {
      irFail(`fixed field ${begin.name} has a variable-length composite`);
    }
    if (begin.presence === 2 && type.presence !== 2) {
      if (type.kind !== "enum") {
        irFail(`constant field ${begin.name} has no constant type encoding`);
      }
      const valueRef = utf8(
        bytesFromHex(begin.constValue),
        `field ${begin.name} value reference`,
      );
      const separator = valueRef.lastIndexOf(".");
      const typeName = valueRef.slice(0, separator);
      const valueName = valueRef.slice(separator + 1);
      const candidate = type.values.find((value) => value.name === valueName);
      if (separator <= 0 || typeName !== type.name || candidate === undefined) {
        irFail(`constant field ${begin.name} has invalid valueRef ${valueRef}`);
      }
      type = Object.freeze({
        ...type,
        presence: 2,
        constValue: candidate.constValue,
      });
    }
    return {
      plan: Object.freeze({
        kind: "field",
        name: begin.name,
        id: begin.fieldId,
        offset: begin.offset,
        encodedLength: begin.encodedLength,
        version: begin.version,
        type,
      }),
      next: endIndex + 1,
    };
  }

  function compileVarData(tokens, index, depth, limits) {
    const { begin, endIndex } = component(
      tokens,
      index,
      SIGNAL.BEGIN_VAR_DATA,
      SIGNAL.END_VAR_DATA,
      "variable data",
    );
    const compiled = compileType(tokens, index + 1, depth, limits);
    if (compiled.next !== endIndex || compiled.plan.kind !== "composite") {
      irFail(
        `variable data ${begin.name} must contain exactly one composite encoding`,
      );
    }
    if (!begin.name || begin.fieldId < 0) {
      irFail(`variable data token ${index} has an invalid identity`);
    }
    if (compiled.plan.members.length !== 2) {
      irFail(`variable data ${begin.name} must contain exactly two encodings`);
    }
    const length = compiled.plan.members.find((member) =>
      member.name === "length"
    );
    const data = compiled.plan.members.find((member) =>
      member.name === "varData"
    );
    if (
      !length || length.kind !== "encoding" || !data || data.kind !== "encoding"
    ) {
      irFail(
        `variable data ${begin.name} is missing length or varData encodings`,
      );
    }
    if (
      ![PRIMITIVE.UINT8, PRIMITIVE.UINT16, PRIMITIVE.UINT32, PRIMITIVE.UINT64]
        .includes(length.primitiveType)
    ) {
      irFail(`variable data ${begin.name} length is not unsigned`);
    }
    if (
      length.presence !== 0 || length.arrayLength !== 1 ||
      length.encodedLength !== length.width || length.offset < 0
    ) {
      irFail(
        `variable data ${begin.name} has an invalid scalar length encoding`,
      );
    }
    if (
      data.primitiveType !== PRIMITIVE.CHAR &&
      data.primitiveType !== PRIMITIVE.UINT8
    ) {
      irFail(`variable data ${begin.name} payload is not byte encoded`);
    }
    if (
      data.presence !== 0 || data.encodedLength !== -1 ||
      data.offset < length.offset + length.encodedLength
    ) {
      irFail(`variable data ${begin.name} has an invalid payload encoding`);
    }
    return {
      plan: Object.freeze({
        kind: "data",
        name: begin.name,
        id: begin.fieldId,
        version: begin.version,
        length,
        data,
      }),
      next: endIndex + 1,
    };
  }

  function compileGroup(tokens, index, depth, limits) {
    if (depth > limits.maxNestingDepth) {
      irFail("schema nesting exceeds its bound");
    }
    const { begin, endIndex } = component(
      tokens,
      index,
      SIGNAL.BEGIN_GROUP,
      SIGNAL.END_GROUP,
      "group",
    );
    if (!begin.name || begin.fieldId < 0) {
      irFail(`group token ${index} has an invalid identity`);
    }
    const dimensions = compileType(tokens, index + 1, depth + 1, limits);
    if (dimensions.plan.kind !== "composite") {
      irFail(`group ${begin.name} has no dimension composite`);
    }
    if (dimensions.plan.encodedLength < 0) {
      irFail(`group ${begin.name} has a variable-length dimension header`);
    }
    const blockLength = dimensions.plan.members.find((member) =>
      member.name === "blockLength"
    );
    const numInGroup = dimensions.plan.members.find((member) =>
      member.name === "numInGroup"
    );
    if (
      !blockLength || !numInGroup || blockLength.kind !== "encoding" ||
      numInGroup.kind !== "encoding"
    ) {
      irFail(
        `group ${begin.name} dimension header is missing blockLength or numInGroup`,
      );
    }
    for (
      const [name, member] of [
        ["blockLength", blockLength],
        ["numInGroup", numInGroup],
      ]
    ) {
      if (
        ![
          PRIMITIVE.UINT8,
          PRIMITIVE.UINT16,
          PRIMITIVE.UINT32,
          PRIMITIVE.UINT64,
        ].includes(member.primitiveType) || member.presence !== 0 ||
        member.arrayLength !== 1 || member.offset < 0
      ) {
        irFail(`group ${begin.name} has an invalid ${name} encoding`);
      }
    }
    const fields = [];
    const variable = [];
    const names = new Set();
    let cursor = dimensions.next;
    let variableStarted = false;
    while (cursor < endIndex) {
      const signal = tokens[cursor].signal;
      let compiled;
      if (signal === SIGNAL.BEGIN_FIELD) {
        if (variableStarted) {
          irFail(`group ${begin.name} has a fixed field after variable data`);
        }
        compiled = compileField(tokens, cursor, depth + 1, limits);
        fields.push(compiled.plan);
      } else if (signal === SIGNAL.BEGIN_GROUP) {
        variableStarted = true;
        compiled = compileGroup(tokens, cursor, depth + 1, limits);
        variable.push(compiled.plan);
      } else if (signal === SIGNAL.BEGIN_VAR_DATA) {
        variableStarted = true;
        compiled = compileVarData(tokens, cursor, depth + 1, limits);
        variable.push(compiled.plan);
      } else {
        irFail(`group ${begin.name} contains unexpected token ${cursor}`);
      }
      if (names.has(compiled.plan.name)) {
        irFail(`group ${begin.name} has duplicate field ${compiled.plan.name}`);
      }
      names.add(compiled.plan.name);
      cursor = compiled.next;
    }
    if (cursor !== endIndex) {
      irFail(`group ${begin.name} crosses its component boundary`);
    }
    return {
      plan: Object.freeze({
        kind: "group",
        name: begin.name,
        id: begin.fieldId,
        version: begin.version,
        dimensions: dimensions.plan,
        blockLength,
        numInGroup,
        fields: Object.freeze(fields),
        variable: Object.freeze(variable),
      }),
      next: endIndex + 1,
    };
  }

  function compileMessage(tokens, index, limits) {
    const { begin, endIndex } = component(
      tokens,
      index,
      SIGNAL.BEGIN_MESSAGE,
      SIGNAL.END_MESSAGE,
      "message",
    );
    if (!begin.name || begin.fieldId < 0 || begin.encodedLength < 0) {
      irFail(`message token ${index} has invalid identity or block length`);
    }
    const fields = [];
    const variable = [];
    const names = new Set();
    let cursor = index + 1;
    let variableStarted = false;
    while (cursor < endIndex) {
      const signal = tokens[cursor].signal;
      let compiled;
      if (signal === SIGNAL.BEGIN_FIELD) {
        if (variableStarted) {
          irFail(`message ${begin.name} has a fixed field after variable data`);
        }
        compiled = compileField(tokens, cursor, 1, limits);
        fields.push(compiled.plan);
      } else if (signal === SIGNAL.BEGIN_GROUP) {
        variableStarted = true;
        compiled = compileGroup(tokens, cursor, 1, limits);
        variable.push(compiled.plan);
      } else if (signal === SIGNAL.BEGIN_VAR_DATA) {
        variableStarted = true;
        compiled = compileVarData(tokens, cursor, 1, limits);
        variable.push(compiled.plan);
      } else {
        irFail(`message ${begin.name} contains unexpected token ${cursor}`);
      }
      if (names.has(compiled.plan.name)) {
        irFail(
          `message ${begin.name} has duplicate field ${compiled.plan.name}`,
        );
      }
      names.add(compiled.plan.name);
      cursor = compiled.next;
    }
    if (cursor !== endIndex) {
      irFail(`message ${begin.name} crosses its component boundary`);
    }
    return {
      plan: Object.freeze({
        id: begin.fieldId,
        name: begin.name,
        blockLength: begin.encodedLength,
        version: begin.version,
        fields: Object.freeze(fields),
        variable: Object.freeze(variable),
      }),
      next: endIndex + 1,
    };
  }

  function parse(input, options = {}) {
    const limits = checkedLimits(options.limits);
    const bytes = asBytes(input, "SBE IR input");
    if (bytes.length > limits.maxIrBytes) {
      irFail(`file exceeds the ${limits.maxIrBytes}-byte bound`);
    }
    if (bytes.length < FRAME_FIXED_LENGTH + 6) irFail("truncated frame");
    const reader = new Reader(bytes);
    const budget = { total: 0, maximum: limits.maxTotalTokenDataBytes };
    const id = reader.i32("frame IR ID");
    const irVersion = reader.i32("frame IR version");
    const version = reader.i32("frame schema version");
    const packageName = utf8(
      reader.variableBytes("frame package name", budget),
      "frame package name",
    );
    const namespaceName = utf8(
      reader.variableBytes("frame namespace name", budget),
      "frame namespace name",
    );
    const semanticVersion = utf8(
      reader.variableBytes("frame semantic version", budget),
      "frame semantic version",
    );
    if (id < 0) irFail("frame IR ID is negative");
    if (irVersion !== IR_VERSION) irFail(`unsupported IR version ${irVersion}`);
    if (version < 0) irFail("frame schema version is negative");

    const tokens = [];
    while (reader.offset < reader.end) {
      if (tokens.length >= limits.maxTokens) {
        irFail(`token count exceeds ${limits.maxTokens}`);
      }
      tokens.push(parseToken(reader, tokens.length, budget));
    }
    if (tokens.length === 0) irFail("token stream is empty");

    const header = compileComposite(tokens, 0, 1, limits);
    if (header.next <= 0 || header.plan.name !== "messageHeader") {
      irFail("first token component is not messageHeader");
    }
    if (header.plan.encodedLength <= 0) {
      irFail("messageHeader has an invalid encoded length");
    }
    const headerNames = new Set(
      header.plan.members.map((member) => member.name),
    );
    for (const name of ["blockLength", "templateId", "schemaId", "version"]) {
      if (!headerNames.has(name)) irFail(`messageHeader is missing ${name}`);
    }

    const messages = [];
    const byId = new Map();
    let cursor = header.next;
    while (cursor < tokens.length) {
      if (messages.length >= limits.maxMessages) {
        irFail(`message count exceeds ${limits.maxMessages}`);
      }
      if (tokens[cursor].signal !== SIGNAL.BEGIN_MESSAGE) {
        irFail(`orphan token ${cursor} follows messageHeader`);
      }
      const compiled = compileMessage(tokens, cursor, limits);
      if (byId.has(compiled.plan.id)) {
        irFail(`duplicate message template ID ${compiled.plan.id}`);
      }
      byId.set(compiled.plan.id, compiled.plan);
      messages.push(compiled.plan);
      cursor = compiled.next;
    }
    if (messages.length === 0) irFail("schema contains no messages");

    const publicMessages = Object.freeze(
      messages.map((message) =>
        Object.freeze({
          id: message.id,
          name: message.name,
          blockLength: message.blockLength,
          version: message.version,
        })
      ),
    );
    const publicById = new Map(
      publicMessages.map((message) => [message.id, message]),
    );
    const schema = Object.freeze({
      irVersion,
      id,
      version,
      packageName,
      namespaceName,
      semanticVersion,
      headerLength: header.plan.encodedLength,
      messages: publicMessages,
      message(templateId) {
        return publicById.get(templateId) ?? null;
      },
    });
    INTERNAL.set(
      schema,
      Object.freeze({ header: header.plan, messages: byId, limits }),
    );
    return schema;
  }

  function primitiveValue(view, type, offset, littleEndian, description) {
    switch (type) {
      case PRIMITIVE.CHAR:
      case PRIMITIVE.UINT8:
        return view.getUint8(offset);
      case PRIMITIVE.INT8:
        return view.getInt8(offset);
      case PRIMITIVE.INT16:
        return view.getInt16(offset, littleEndian);
      case PRIMITIVE.INT32:
        return view.getInt32(offset, littleEndian);
      case PRIMITIVE.INT64:
        return view.getBigInt64(offset, littleEndian);
      case PRIMITIVE.UINT16:
        return view.getUint16(offset, littleEndian);
      case PRIMITIVE.UINT32:
        return view.getUint32(offset, littleEndian);
      case PRIMITIVE.UINT64:
        return view.getBigUint64(offset, littleEndian);
      case PRIMITIVE.FLOAT:
        return finiteFloat(view.getFloat32(offset, littleEndian));
      case PRIMITIVE.DOUBLE:
        return finiteFloat(view.getFloat64(offset, littleEndian));
      default:
        decodeFail(`${description} has no primitive encoding`);
    }
  }

  function finiteFloat(value) {
    if (Number.isNaN(value)) return "NaN";
    if (value === Infinity) return "Infinity";
    if (value === -Infinity) return "-Infinity";
    return value;
  }

  function primitiveFromHex(value, type, _byteOrder, description) {
    if (!value) return null;
    const bytes = bytesFromHex(value);
    const width = PRIMITIVE_WIDTH[type];
    if (width === 0 || bytes.length % width !== 0) {
      irFail(`${description} has an invalid primitive value length`);
    }
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const values = [];
    for (let offset = 0; offset < bytes.length; offset += width) {
      // Serialized SBE IR stores PrimitiveValue bytes in little-endian order,
      // independently of the application message's declared byte order.
      values.push(primitiveValue(view, type, offset, true, description));
    }
    return values.length === 1 ? values[0] : Object.freeze(values);
  }

  function valueKey(value) {
    return Array.isArray(value)
      ? `[${value.map((entry) => valueKey(entry)).join(",")}]`
      : `${typeof value}:${String(value)}`;
  }

  function defaultNullValue(primitiveType, description) {
    switch (primitiveType) {
      case PRIMITIVE.CHAR:
      case PRIMITIVE.UINT8:
        return primitiveType === PRIMITIVE.CHAR ? 0 : 0xff;
      case PRIMITIVE.INT8:
        return -0x80;
      case PRIMITIVE.INT16:
        return -0x8000;
      case PRIMITIVE.INT32:
        return -0x8000_0000;
      case PRIMITIVE.INT64:
        return -(1n << 63n);
      case PRIMITIVE.UINT16:
        return 0xffff;
      case PRIMITIVE.UINT32:
        return 0xffff_ffff;
      case PRIMITIVE.UINT64:
        return (1n << 64n) - 1n;
      case PRIMITIVE.FLOAT:
      case PRIMITIVE.DOUBLE:
        return "NaN";
      default:
        decodeFail(`${description} has no optional null value`);
    }
  }

  function applicableNullValue(plan, description) {
    if (plan.nullValue) {
      return primitiveFromHex(
        plan.nullValue,
        plan.primitiveType,
        plan.byteOrder,
        `${description} null value`,
      );
    }
    return defaultNullValue(plan.primitiveType, description);
  }

  function record(entries) {
    const value = Object.create(null);
    for (const [name, entry] of entries) {
      Object.defineProperty(value, name, {
        value: entry,
        enumerable: true,
        writable: false,
        configurable: false,
      });
    }
    return Object.freeze(value);
  }

  function requireRange(context, offset, length, description) {
    if (
      !Number.isSafeInteger(offset) || !Number.isSafeInteger(length) ||
      offset < context.start || length < 0 || offset > context.end - length
    ) {
      decodeFail(`truncated ${description}`);
    }
  }

  function decodeEncoding(plan, context, offset, description) {
    if (plan.presence === 2) {
      const constant = primitiveFromHex(
        plan.constValue,
        plan.primitiveType,
        plan.byteOrder,
        `${description} constant`,
      );
      if (plan.primitiveType === PRIMITIVE.CHAR) {
        return decodeCharacterBytes(
          bytesFromHex(plan.constValue),
          plan.characterEncoding,
          description,
          true,
        );
      }
      return constant;
    }
    if (plan.encodedLength < 0) {
      decodeFail(`${description} has variable length in a fixed field`);
    }
    requireRange(context, offset, plan.encodedLength, description);
    const count = plan.arrayLength;
    const values = [];
    for (let index = 0; index < count; index += 1) {
      values.push(primitiveValue(
        context.view,
        plan.primitiveType,
        offset + index * plan.width,
        plan.byteOrder === 0,
        description,
      ));
    }
    const rawValue = count === 1 ? values[0] : Object.freeze(values);
    if (plan.presence === 1) {
      const nullValue = applicableNullValue(plan, description);
      if (valueKey(rawValue) === valueKey(nullValue)) return null;
    }
    if (plan.primitiveType === PRIMITIVE.CHAR) {
      const bytes = context.bytes.subarray(offset, offset + plan.encodedLength);
      return decodeCharacterBytes(
        bytes,
        plan.characterEncoding,
        description,
        true,
      );
    }
    return rawValue;
  }

  function decodeCharacterBytes(
    bytes,
    encoding,
    description,
    stopAtNull = false,
  ) {
    if (stopAtNull) {
      const terminator = bytes.indexOf(0);
      if (terminator >= 0) bytes = bytes.subarray(0, terminator);
    }
    const normalized = encoding.trim().toLowerCase();
    if (normalized === "utf-8" || normalized === "utf8") {
      try {
        return UTF8.decode(bytes);
      } catch (_error) {
        decodeFail(`${description} is not valid UTF-8`);
      }
    }
    if (normalized === "ascii" || normalized === "us-ascii") {
      for (const byte of bytes) {
        if (byte > 0x7f) decodeFail(`${description} is not valid ASCII`);
      }
      return String.fromCharCode(...bytes);
    }
    if (normalized) {
      try {
        return new TextDecoder(encoding, { fatal: true, ignoreBOM: true })
          .decode(bytes);
      } catch (_error) {
        decodeFail(
          `${description} uses unsupported or invalid character encoding ${encoding}`,
        );
      }
    }
    return bytes.length === 1
      ? String.fromCharCode(bytes[0])
      : Object.freeze(Array.from(bytes));
  }

  function decodeType(
    plan,
    context,
    offset,
    actingVersion,
    blockLength,
    description,
  ) {
    switch (plan.kind) {
      case "encoding":
        return decodeEncoding(plan, context, offset, description);
      case "composite": {
        const entries = [];
        for (const member of plan.members) {
          const value = decodeFixedType(
            member,
            context,
            offset,
            actingVersion,
            blockLength,
            `${description}.${member.name}`,
          );
          entries.push([member.name, value]);
        }
        return record(entries);
      }
      case "enum": {
        const raw = plan.presence === 2
          ? primitiveFromHex(
            plan.constValue,
            plan.primitiveType,
            plan.byteOrder,
            `${description} constant`,
          )
          : decodeScalarPlan(plan, context, offset, description);
        if (raw === null) return null;
        let name = null;
        for (const candidate of plan.values) {
          const value = primitiveFromHex(
            candidate.constValue,
            plan.primitiveType,
            plan.byteOrder,
            `${description} enum value`,
          );
          if (valueKey(value) === valueKey(raw)) name = candidate.name;
        }
        return record([["name", name], ["value", raw]]);
      }
      case "set": {
        const raw = plan.presence === 2
          ? primitiveFromHex(
            plan.constValue,
            plan.primitiveType,
            plan.byteOrder,
            `${description} constant`,
          )
          : decodeScalarPlan(plan, context, offset, description);
        if (raw === null) return null;
        const rawBits = BigInt(raw);
        const choices = [];
        for (const candidate of plan.choices) {
          const bit = primitiveFromHex(
            candidate.constValue,
            plan.primitiveType,
            plan.byteOrder,
            `${description} choice`,
          );
          const bitIndex = BigInt(bit);
          if (bitIndex < 0n || bitIndex > 63n) {
            decodeFail(`${description} has an invalid choice bit ${bitIndex}`);
          }
          if ((rawBits & (1n << bitIndex)) !== 0n) choices.push(candidate.name);
        }
        return record([["choices", Object.freeze(choices)], ["value", raw]]);
      }
      default:
        decodeFail(`${description} has unknown compiled type ${plan.kind}`);
    }
  }

  function decodeScalarPlan(plan, context, offset, description) {
    const width = primitiveWidth(plan.primitiveType, description);
    requireRange(context, offset, width, description);
    const raw = primitiveValue(
      context.view,
      plan.primitiveType,
      offset,
      plan.byteOrder === 0,
      description,
    );
    if (plan.presence === 1) {
      const nullValue = applicableNullValue(plan, description);
      if (valueKey(raw) === valueKey(nullValue)) return null;
    }
    return raw;
  }

  function decodeFixedType(
    plan,
    context,
    base,
    actingVersion,
    blockLength,
    description,
  ) {
    if (plan.version > actingVersion) return null;
    if (plan.presence === 2) {
      return decodeType(plan, context, base, actingVersion, 0, description);
    }
    if (plan.offset < 0) {
      decodeFail(`${description} has an invalid fixed offset or length`);
    }
    const available = blockLength - plan.offset;
    if (available < 0) {
      decodeFail(`${description} does not fit the acting block length`);
    }
    if (plan.kind !== "composite" && plan.encodedLength > available) {
      decodeFail(`${description} does not fit the acting block length`);
    }
    if (plan.kind !== "composite" && plan.encodedLength < 0) {
      decodeFail(`${description} has an invalid fixed offset or length`);
    }
    return decodeType(
      plan,
      context,
      base + plan.offset,
      actingVersion,
      plan.kind === "composite"
        ? Math.min(available, plan.encodedLength)
        : plan.encodedLength,
      description,
    );
  }

  function decodeField(
    field,
    context,
    base,
    actingVersion,
    blockLength,
    description,
  ) {
    if (field.version > actingVersion) return null;
    const type = field.type;
    if (type.presence === 2) {
      return decodeType(type, context, base, actingVersion, 0, description);
    }
    if (field.offset < 0) {
      decodeFail(`${description} has an invalid fixed offset or length`);
    }
    const available = blockLength - field.offset;
    if (available < 0) {
      decodeFail(`${description} does not fit the acting block length`);
    }
    if (type.kind !== "composite" && field.encodedLength > available) {
      decodeFail(`${description} does not fit the acting block length`);
    }
    if (type.kind !== "composite" && field.encodedLength < 0) {
      decodeFail(`${description} has an invalid fixed offset or length`);
    }
    return decodeType(
      type,
      context,
      base + field.offset,
      actingVersion,
      type.kind === "composite"
        ? Math.min(available, field.encodedLength)
        : field.encodedLength,
      description,
    );
  }

  function unsignedCount(value, description, maximum) {
    let bigint;
    if (typeof value === "bigint") {
      if (value < 0n) {
        decodeFail(`${description} is not an exact unsigned count`);
      }
      bigint = value;
    } else if (typeof value === "number") {
      if (!Number.isSafeInteger(value) || value < 0) {
        decodeFail(`${description} is not an exact unsigned count`);
      }
      bigint = BigInt(value);
    } else if (typeof value === "string" && /^(?:0|[1-9][0-9]*)$/.test(value)) {
      bigint = BigInt(value);
    } else {
      decodeFail(`${description} is not an exact unsigned count`);
    }
    if (bigint > MAX_SAFE_BIGINT || bigint > BigInt(maximum)) {
      decodeFail(`${description} exceeds ${maximum}`);
    }
    return Number(bigint);
  }

  function decodeData(plan, state, actingVersion, description) {
    if (plan.version > actingVersion) return null;
    const start = state.cursor;
    const lengthOffset = start + plan.length.offset;
    const rawLength = decodeEncoding(
      plan.length,
      state.context,
      lengthOffset,
      `${description} length`,
    );
    const length = unsignedCount(
      rawLength,
      `${description} length`,
      state.limits.maxVarDataBytes,
    );
    const dataOffset = start + plan.data.offset;
    requireRange(state.context, dataOffset, length, description);
    const bytes = state.context.bytes.subarray(dataOffset, dataOffset + length);
    state.cursor = dataOffset + length;
    if (plan.data.characterEncoding) {
      return decodeCharacterBytes(
        bytes,
        plan.data.characterEncoding,
        description,
      );
    }
    return bytes.slice();
  }

  function decodeGroup(plan, state, actingVersion, depth, description) {
    if (plan.version > actingVersion) return Object.freeze([]);
    if (depth > state.limits.maxNestingDepth) {
      decodeFail("group nesting exceeds its bound");
    }
    const dimensionStart = state.cursor;
    requireRange(
      state.context,
      dimensionStart,
      plan.dimensions.encodedLength,
      `${description} dimensions`,
    );
    const blockLengthValue = decodeFixedType(
      plan.blockLength,
      state.context,
      dimensionStart,
      actingVersion,
      plan.dimensions.encodedLength,
      `${description}.blockLength`,
    );
    const countValue = decodeFixedType(
      plan.numInGroup,
      state.context,
      dimensionStart,
      actingVersion,
      plan.dimensions.encodedLength,
      `${description}.numInGroup`,
    );
    const groupBlockLength = unsignedCount(
      blockLengthValue,
      `${description} block length`,
      state.limits.maxMessageBytes,
    );
    const count = unsignedCount(
      countValue,
      `${description} count`,
      state.limits.maxGroupEntries,
    );
    state.totalGroupEntries += count;
    if (state.totalGroupEntries > state.limits.maxTotalGroupEntries) {
      decodeFail(
        `total group entries exceed ${state.limits.maxTotalGroupEntries}`,
      );
    }
    state.cursor += plan.dimensions.encodedLength;
    const rows = [];
    for (let index = 0; index < count; index += 1) {
      const rowStart = state.cursor;
      requireRange(
        state.context,
        rowStart,
        groupBlockLength,
        `${description}[${index}] fixed block`,
      );
      const entries = [];
      for (const field of plan.fields) {
        entries.push([
          field.name,
          decodeField(
            field,
            state.context,
            rowStart,
            actingVersion,
            groupBlockLength,
            `${description}[${index}].${field.name}`,
          ),
        ]);
      }
      state.cursor = rowStart + groupBlockLength;
      for (const variable of plan.variable) {
        const value = variable.kind === "group"
          ? decodeGroup(
            variable,
            state,
            actingVersion,
            depth + 1,
            `${description}[${index}].${variable.name}`,
          )
          : decodeData(
            variable,
            state,
            actingVersion,
            `${description}[${index}].${variable.name}`,
          );
        entries.push([variable.name, value]);
      }
      rows.push(record(entries));
    }
    return Object.freeze(rows);
  }

  function headerNumber(value, description) {
    if (
      typeof value !== "number" || !Number.isSafeInteger(value) || value < 0
    ) {
      decodeFail(
        `message header ${description} is not a safe unsigned integer`,
      );
    }
    return value;
  }

  function decodeHeader(schema, input, options = {}) {
    const internal = INTERNAL.get(schema);
    if (internal === undefined) {
      throw new TypeError("schema must be returned by SbeIr.parse");
    }
    const bytes = asBytes(input, "SBE message input");
    const offset = options.offset ?? 0;
    const end = options.end ?? bytes.length;
    if (
      !Number.isSafeInteger(offset) || !Number.isSafeInteger(end) ||
      offset < 0 || end < offset || end > bytes.length
    ) {
      throw new RangeError("SBE message offset/end are outside the input");
    }
    const context = {
      bytes,
      view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
      start: offset,
      end,
    };
    requireRange(
      context,
      offset,
      internal.header.encodedLength,
      "message header",
    );
    const value = decodeType(
      internal.header,
      context,
      offset,
      schema.version,
      internal.header.encodedLength,
      "messageHeader",
    );
    const header = Object.freeze({
      blockLength: headerNumber(value.blockLength, "blockLength"),
      templateId: headerNumber(value.templateId, "templateId"),
      schemaId: headerNumber(value.schemaId, "schemaId"),
      version: headerNumber(value.version, "version"),
    });
    return Object.freeze({
      header,
      value,
      bytesRead: internal.header.encodedLength,
      endOffset: offset + internal.header.encodedLength,
    });
  }

  function decodeMessage(schema, input, options = {}) {
    const internal = INTERNAL.get(schema);
    if (internal === undefined) {
      throw new TypeError("schema must be returned by SbeIr.parse");
    }
    const limits = checkedLimits({
      ...internal.limits,
      ...(options.limits ?? {}),
    });
    const bytes = asBytes(input, "SBE message input");
    const offset = options.offset ?? 0;
    const end = options.end ?? bytes.length;
    if (
      !Number.isSafeInteger(offset) || !Number.isSafeInteger(end) ||
      offset < 0 || end < offset || end > bytes.length
    ) {
      throw new RangeError("SBE message offset/end are outside the input");
    }
    if (end - offset > limits.maxMessageBytes) {
      decodeFail(`message exceeds the ${limits.maxMessageBytes}-byte bound`);
    }
    const context = {
      bytes,
      view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
      start: offset,
      end,
    };
    const decodedHeader = decodeHeader(schema, bytes, { offset, end });
    const { blockLength, templateId, schemaId, version: actingVersion } =
      decodedHeader.header;
    if (schemaId !== schema.id) {
      decodeFail(`schema ID ${schemaId} does not match ${schema.id}`);
    }
    const message = internal.messages.get(templateId);
    if (message === undefined) decodeFail(`unknown template ID ${templateId}`);
    const body = offset + internal.header.encodedLength;
    requireRange(context, body, blockLength, `${message.name} fixed block`);
    const entries = [];
    for (const field of message.fields) {
      entries.push([
        field.name,
        decodeField(
          field,
          context,
          body,
          actingVersion,
          blockLength,
          `${message.name}.${field.name}`,
        ),
      ]);
    }
    const state = {
      context,
      cursor: body + blockLength,
      limits,
      totalGroupEntries: 0,
    };
    for (const variable of message.variable) {
      const value = variable.kind === "group"
        ? decodeGroup(
          variable,
          state,
          actingVersion,
          1,
          `${message.name}.${variable.name}`,
        )
        : decodeData(
          variable,
          state,
          actingVersion,
          `${message.name}.${variable.name}`,
        );
      entries.push([variable.name, value]);
    }
    if (options.requireExactLength !== false && state.cursor !== end) {
      decodeFail(`${message.name} has ${end - state.cursor} trailing bytes`);
    }
    return Object.freeze({
      header: decodedHeader.header,
      template: schema.message(templateId),
      value: record(entries),
      bytesRead: state.cursor - offset,
      endOffset: state.cursor,
    });
  }

  root.SbeIr = Object.freeze({
    parse,
    decodeBase64,
    decodeHeader,
    decodeMessage,
    SbeIrError,
    SbeDecodeError,
    irVersion: IR_VERSION,
  });
})(globalThis);

#!/usr/bin/env -S deno run --no-config

const [globalName, sbeToolVersion, inputPath, outputPath] = Deno.args;

if (
  Deno.args.length !== 4 ||
  !/^[A-Za-z_$][A-Za-z0-9_$]*$/.test(globalName) ||
  sbeToolVersion.length === 0
) {
  console.error(
    "usage: generate-sbe-ir-module.js <global-name> <sbe-tool-version> <input.sbeir> <output.js>",
  );
  Deno.exit(2);
}

const bytes = await Deno.readFile(inputPath);
const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
const sha256 = Array.from(digest, (byte) => byte.toString(16).padStart(2, "0"))
  .join("");

let binary = "";
for (let offset = 0; offset < bytes.length; offset += 0x8000) {
  binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
}
const base64 = btoa(binary);
const chunks = base64.match(/.{1,100}/g) ?? [""];
const encoded = chunks
  .map((chunk, index) =>
    `    ${JSON.stringify(chunk)}${index + 1 === chunks.length ? "" : " +"}`
  )
  .join("\n");

const source =
  `// Generated from the official SBE intermediate representation. Do not edit.\n` +
  `(function (root) {\n` +
  `  "use strict";\n\n` +
  `  root.${globalName} = Object.freeze({\n` +
  `    sbeToolVersion: ${JSON.stringify(sbeToolVersion)},\n` +
  `    sha256: ${JSON.stringify(sha256)},\n` +
  `    byteLength: ${bytes.length},\n` +
  `    base64:\n${encoded}\n` +
  `  });\n` +
  `})(globalThis);\n`;

await Deno.writeTextFile(outputPath, source);

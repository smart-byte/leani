//! The TypeScript a spec generates: the processor its Rust is, in the
//! TypeScript SDK's terms, keeping the same rows and totals.

use std::fmt::Write as _;

use super::{Data, Generator, Keep, type_name};

impl Generator<'_> {
    pub(super) fn typescript(&self) -> Result<String, String> {
        let spec = self.spec;
        let class = type_name(&spec.name);
        let mut out = self.header("//", "npm install && npm run build")?;
        let mut body = self.typescript_constants();
        let _ = write!(body, "\nclass {class} extends Processor {{\n");
        body.push_str(&self.typescript_manifest());
        body.push_str(&self.typescript_map());
        body.push_str(&self.typescript_reduce());
        body.push_str("}\n");
        body.push_str(&self.typescript_helpers());
        let _ = writeln!(body, "\nregister(new {class}());");
        out.push_str(&typescript_imports(&body));
        out.push_str(&body);
        Ok(out)
    }

    fn typescript_constants(&self) -> String {
        let mut text = String::new();
        match &self.spec.data {
            Data::Events {
                contracts, events, ..
            } => {
                text.push_str(
                    "/** Contracts whose events count; empty counts every contract. */\n",
                );
                text.push_str(&list("CONTRACTS", contracts));
                text.push_str("/** The events to decode, as Solidity declares them. */\n");
                text.push_str(&list("EVENTS", events));
            }
            Data::Transactions {
                to,
                from,
                functions,
                ..
            } => {
                text.push_str("/** Recipients to watch; empty watches every transaction. */\n");
                text.push_str(&list("TO", to));
                text.push_str("/** Senders to watch; empty watches every sender. */\n");
                text.push_str(&list("FROM", from));
                if !functions.is_empty() {
                    text.push_str(
                        "/** The functions to decode calls of, as Solidity declares them. */\n",
                    );
                    text.push_str(&list("FUNCTIONS", functions));
                }
            }
            Data::Blocks { .. } => {}
        }
        if let Some(arguments) = self.kept_arguments() {
            text.push_str("/** The arguments each row keeps. */\n");
            text.push_str(&list("ARGUMENTS", &arguments));
        }
        if let Keep::Totals { sum, .. } = &self.spec.keep {
            text.push_str("/** The fields each total adds up. */\n");
            text.push_str(&list("SUMS", sum));
        }
        text
    }

    fn typescript_manifest(&self) -> String {
        let name = &self.spec.name;
        let mut text = String::from("  manifest(params: Json): Manifest {\n");
        let mut chain = Vec::new();
        match &self.spec.data {
            Data::Events { .. } => {
                text.push_str(
                    "    // The node keeps only these contracts' logs with these topics.\n",
                );
                chain.push(if self.keeps("transactionHash") {
                    ".logsWithTransactionHash()".to_owned()
                } else {
                    ".require(Capability.Logs)".to_owned()
                });
                chain.push(".addresses(addresses(CONTRACTS))".to_owned());
                chain.push(
                    ".topic(0, events().map<Uint8Array>((event: Signature) => event.topic()))"
                        .to_owned(),
                );
            }
            Data::Transactions {
                functions, from, ..
            } => {
                text.push_str(
                    "    // The node keeps only transactions to and from these accounts.\n",
                );
                for (capability, why) in self.transaction_requirements(functions, from) {
                    chain.push(format!("// {why}"));
                    chain.push(format!(".require(Capability.{capability})"));
                }
                chain.push(".recipients(addresses(TO))".to_owned());
                chain.push(".senders(addresses(FROM))".to_owned());
            }
            Data::Blocks { .. } => {
                chain.push(".require(Capability.Header)".to_owned());
                if self.keeps("transactionCount") || self.keeps("sizeBytes") {
                    chain.push("// transaction count and size".to_owned());
                    chain.push(".require(Capability.BlockSize)".to_owned());
                }
            }
        }
        if matches!(self.spec.keep, Keep::Totals { .. }) {
            chain.push(
                "// Totals read what earlier blocks added: reduce in chain order.".to_owned(),
            );
            chain.push(".ordered()".to_owned());
        }
        let _ = write!(text, "    return new Manifest(\"{name}\")");
        for link in chain {
            let _ = write!(text, "\n      {link}");
        }
        text.push_str(";\n  }\n\n");
        text
    }

    fn typescript_set(&self, indent: &str, name: &str, value: &str) -> String {
        if self.keeps(name) {
            format!("{indent}fields.set(\"{name}\", {value});\n")
        } else {
            String::new()
        }
    }

    fn typescript_arguments(&self, signature: &str, values: &str) -> String {
        let filter = if self.kept_arguments().is_some() {
            "\n        if (!ARGUMENTS.includes(name)) continue;"
        } else {
            ""
        };
        format!(
            "      for (let argument = 0; argument < {signature}.inputs.length; argument++) {{\n        \
             const name = {signature}.inputs[argument].name;{filter}\n        \
             fields.set(name, {values}[argument]);\n      }}\n"
        )
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one arm per kind of data, each the text of its map"
    )]
    fn typescript_map(&self) -> String {
        let mut text = String::new();
        match &self.spec.data {
            Data::Events { .. } => {
                text.push_str(
                    "  // One row per match: its key in the collection, then its fields as JSON.\n  \
                     map(params: Json, block: Block, rows: Writer): void {\n    \
                     const signatures = events();\n    \
                     const topics = signatures.map<Uint8Array>((event: Signature) => event.topic());\n    \
                     for (let index = 0; index < block.logs.length; index++) {\n      \
                     const log = block.logs[index];\n      \
                     const position = log.topics.length > 0 ? indexOf(topics, log.topics[0]) : -1;\n      \
                     if (position < 0) continue;\n      \
                     const event = signatures[position];\n      \
                     // Another contract may emit the topic with another layout: a log\n      \
                     // that does not decode is not this event.\n      \
                     const args = event.decodeEvent(log.topics.slice(1), log.data);\n      \
                     if (args == null) continue;\n      \
                     const fields = Json.object();\n",
                );
                text.push_str(&self.typescript_set("      ", "event", "event.name"));
                text.push_str(&self.typescript_set("      ", "contract", "log.address"));
                text.push_str(&self.typescript_set("      ", "logIndex", "log.logIndex"));
                text.push_str(&self.typescript_set(
                    "      ",
                    "transactionHash",
                    "log.transactionHash",
                ));
                text.push_str(&self.typescript_arguments("event", "args"));
                text.push_str(
                    "      // A log index is unique within its block.\n      \
                     rows.bytes(concat(bigEndian64(block.number), bigEndian32(log.logIndex))).text(fields.toString());\n    \
                     }\n  }\n\n",
                );
            }
            Data::Transactions { functions, .. } => {
                text.push_str(
                    "  // One row per match: its key in the collection, then its fields as JSON.\n  \
                     map(params: Json, block: Block, rows: Writer): void {\n",
                );
                if !functions.is_empty() {
                    text.push_str(
                        "    const signatures = functions();\n    \
                         const selectors = signatures.map<Uint8Array>((call: Signature) => call.selector());\n",
                    );
                }
                text.push_str(
                    "    for (let index = 0; index < block.transactions.length; index++) {\n      \
                     const transaction = block.transactions[index];\n",
                );
                if !functions.is_empty() {
                    text.push_str(
                        "      const input = transaction.input;\n      \
                         const position = input != null && input.length >= 4 ? indexOf(selectors, input.slice(0, 4)) : -1;\n      \
                         if (input == null || position < 0) continue;\n      \
                         const call = signatures[position];\n      \
                         const args = call.decodeArguments(input.slice(4));\n      \
                         if (args == null) continue;\n",
                    );
                }
                text.push_str("      const fields = Json.object();\n");
                text.push_str(&self.typescript_set("      ", "hash", "transaction.hash"));
                text.push_str(&self.typescript_set("      ", "from", "transaction.from"));
                text.push_str(&self.typescript_set("      ", "to", "transaction.to"));
                text.push_str(&self.typescript_set("      ", "value", "transaction.value"));
                if self.keeps_outcome() {
                    text.push_str(
                        "      // Receipts belong to the same transactions; match them by index.\n      \
                         const receipt = receiptOf(block, transaction.index);\n      \
                         if (receipt != null) {\n",
                    );
                    for (name, value) in [
                        ("success", "receipt.success"),
                        ("gasUsed", "receipt.gasUsed"),
                        ("effectiveGasPrice", "receipt.effectiveGasPrice"),
                    ] {
                        text.push_str(&self.typescript_set("        ", name, value));
                    }
                    text.push_str("      }\n");
                }
                if !functions.is_empty() {
                    text.push_str(&self.typescript_set("      ", "function", "call.name"));
                    text.push_str(&self.typescript_arguments("call", "args"));
                }
                text.push_str(
                    "      rows.bytes(transaction.hash).text(fields.toString());\n    }\n  }\n\n",
                );
            }
            Data::Blocks { .. } => {
                text.push_str(
                    "  // One row per block: its number, then its fields as JSON.\n  \
                     map(params: Json, block: Block, rows: Writer): void {\n    \
                     const found = block.header;\n    \
                     const header = found != null ? found : new Header();\n    \
                     const fields = Json.object();\n",
                );
                for name in [
                    "gasUsed",
                    "gasLimit",
                    "baseFeePerGas",
                    "blobGasUsed",
                    "transactionCount",
                    "sizeBytes",
                ] {
                    text.push_str(&self.typescript_set("    ", name, &format!("header.{name}")));
                }
                text.push_str(
                    "    rows.bytes(bigEndian64(block.number)).text(fields.toString());\n  }\n\n",
                );
            }
        }
        text
    }

    fn typescript_reduce(&self) -> String {
        let (collection, kind) = self.collection();
        let mut text = String::from(
            "  reduce(params: Json, block: BlockRef, rows: Reader, store: Store): void {\n    \
             while (!rows.done) {\n",
        );
        match &self.spec.keep {
            Keep::Rows => {
                let _ = write!(
                    text,
                    "      const key = rows.bytes();\n      \
                     const fields = Json.parse(rows.text());\n      \
                     fields.set(\"blockNumber\", block.number).set(\"timestamp\", block.timestamp);\n      \
                     store.upsert(\"{collection}\", \"{kind}\", key, fields);\n"
                );
            }
            Keep::Totals { by, .. } => {
                text.push_str(
                    "      // A total's key is its group, not the row's.\n      \
                     rows.bytes();\n      \
                     const fields = Json.parse(rows.text());\n",
                );
                match by {
                    Some(by) => {
                        let _ = write!(
                            text,
                            "      // One total per `{by}`.\n      \
                             const value = fields.get(\"{by}\");\n      \
                             const group = value == null ? \"none\" : value.toText();\n"
                        );
                    }
                    None => text.push_str(
                        "      // One total of every match.\n      const group = \"all\";\n",
                    ),
                }
                let label = by.as_deref().unwrap_or("group");
                let _ = write!(
                    text,
                    "      const stored = store.getJson(\"totals\", utf8(group));\n      \
                     const total = stored != null ? stored : Json.object();\n      \
                     const count = total.get(\"count\");\n      \
                     const previous: u64 = count == null ? 0 : count.asU64();\n      \
                     total.set(\"{label}\", group).set(\"count\", previous + 1);\n      \
                     for (let index = 0; index < SUMS.length; index++) add(total, fields, SUMS[index]);\n      \
                     total.set(\"lastBlock\", block.number);\n      \
                     store.upsert(\"totals\", \"total\", utf8(group), total);\n"
                );
            }
        }
        text.push_str("    }\n  }\n");
        text
    }

    fn typescript_helpers(&self) -> String {
        let mut text = String::new();
        match &self.spec.data {
            Data::Events { .. } => text.push_str(
                "\n/** Each event's signature. */\nfunction events(): Signature[] {\n  \
                 return EVENTS.map<Signature>((text: string) => Signature.parse(text));\n}\n",
            ),
            Data::Transactions { functions, .. } if !functions.is_empty() => text.push_str(
                "\n/** Each function's signature. */\nfunction functions(): Signature[] {\n  \
                 return FUNCTIONS.map<Signature>((text: string) => Signature.parse(text));\n}\n",
            ),
            _ => {}
        }
        if !matches!(self.spec.data, Data::Blocks { .. }) {
            text.push_str(
                "\n/** The addresses in a list of `0x` hex. */\nfunction addresses(list: string[]): Uint8Array[] {\n  \
                 return list.map<Uint8Array>((address: string) => parseHex(address, 20));\n}\n",
            );
        }
        if matches!(self.spec.data, Data::Transactions { .. }) && self.keeps_outcome() {
            text.push_str(
                "\n/** The receipt of the transaction at `index`, if the block has it. */\n\
                 function receiptOf(block: Block, index: u32): Receipt | null {\n  \
                 for (let position = 0; position < block.receipts.length; position++) {\n    \
                 const receipt = block.receipts[position];\n    \
                 if (receipt.transactionIndex == index) return receipt;\n  }\n  return null;\n}\n",
            );
        }
        if matches!(self.spec.keep, Keep::Totals { .. }) {
            text.push_str(
                "\n/** Add `fields[field]`, an unsigned integer, to the running sum `total[field]`. */\n\
                 function add(total: Json, fields: Json, field: string): void {\n  \
                 const value = fields.get(field);\n  \
                 if (value == null || value.isNull) return;\n  \
                 const sum = total.get(field);\n  \
                 total.set(field, (sum == null ? U256.zero() : U256.fromJson(sum)).saturatingAdd(U256.fromJson(value)));\n}\n",
            );
        }
        text
    }
}

fn typescript_imports(body: &str) -> String {
    let mut sdk = [
        "Block",
        "BlockRef",
        "Json",
        "Manifest",
        "Processor",
        "Reader",
        "Store",
        "Writer",
        "register",
    ]
    .to_vec();
    for (name, used) in [
        ("Capability", "Capability."),
        ("Header", "new Header("),
        ("Receipt", "Receipt | null"),
        ("U256", "U256."),
        ("bigEndian32", "bigEndian32("),
        ("bigEndian64", "bigEndian64("),
        ("concat", "concat("),
        ("indexOf", "indexOf("),
        ("parseHex", "parseHex("),
        ("utf8", "utf8("),
    ] {
        if body.contains(used) {
            sdk.push(name);
        }
    }
    // Types, then functions, as their case orders them.
    sdk.sort_unstable();
    let one_line = format!("import {{ {} }} from \"./leani\";\n", sdk.join(", "));
    let mut text = if one_line.len() <= 120 {
        one_line
    } else {
        let mut lines = String::from("import {\n");
        for name in sdk {
            let _ = writeln!(lines, "  {name},");
        }
        lines + "} from \"./leani\";\n"
    };
    if body.contains("Signature") {
        text.push_str("import { Signature } from \"./leani/solidity\";\n");
    }
    text.push_str("export * from \"./leani/exports\";\n\n");
    text
}

/// `const NAME: string[] = [...];`
fn list(name: &str, items: &[impl AsRef<str>]) -> String {
    let items = items
        .iter()
        .map(|item| serde_json::to_string(item.as_ref()).unwrap_or_default())
        .collect::<Vec<_>>();
    let one_line = format!("const {name}: string[] = [{}];\n", items.join(", "));
    if one_line.len() <= 120 {
        return one_line;
    }
    let mut text = format!("const {name}: string[] = [\n");
    for item in items {
        let _ = writeln!(text, "  {item},");
    }
    text.push_str("];\n");
    text
}

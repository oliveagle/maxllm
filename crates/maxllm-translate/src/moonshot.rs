// Copyright 2025 MaxLLM Contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Moonshot/Kimi tool-schema projection — the dialect repair the upstream
//! demands before it accepts a tool definition.
//!
//! Kimi validates every `tools[].function.parameters` against a restricted
//! JSON-Schema dialect ("moonshot flavored json schema") and answers
//!
//! ```text
//! 400 {"error":{"code":"invalid_request_error","message":"Invalid request:
//! tools.function.parameters is not a valid moonshot flavored json schema,
//! details: <At path 'properties.show': detected infinite recursion without
//! termination condition>"}}
//!
//! for shapes other providers happily take. The violations seen in the wild
//! are the *zod 4* shapes codex sends (`$defs.__schemaN` entries referenced
//! with a `$ref`):
//!
//! * **a `$ref` beside another keyword** — answered
//!   `<At path '$defs.__schemaN': when using $ref, type should be defined in
//!   the referenced schema instead of the parent schema>`. The referenced
//!   keyword belongs to the *target*, so the shape is rewritten by inlining
//!   the target's keywords at the reference site (the site's own keywords
//!   win — zod's `z.number().min(0)` reused and refined with `.int()` must
//!   stay `integer`, not fall back to the target's `number`).
//! * **a `$ref` whose target carries no `type`** — answered either
//!   `type is not defined` or, when the chain never reaches a typed node,
//!   `detected infinite recursion without termination condition`. The
//!   keyword is inferred (properties → object, items → array, the shared
//!   `enum`/`const`/branch value type) and a cycle is cut at a terminating
//!   object — the walk Moonshot runs cannot end otherwise.
//! * **a `type` beside an `anyOf`/`oneOf`/`allOf`** — answered `type should
//!   be defined in anyOf items`; the keyword is dropped from the parent and
//!   given to the branches that lack one.
//! * **an `enum`/`const` without a `type`** — the value's type is added.
//!
//! And one last guard before a body is forwarded (see [`normalize_tools`]):
//! a reference cycle that survives the projection — hidden under an
//! applicator keyword the dialect walk does not enter, or closed through a
//! plain JSON pointer outside `$defs` — degrades the tool schema to a bare
//! object. The upstream answers a cycle with `detected infinite recursion
//! without termination condition`, the one rejection that ends an agent's
//! turn; the gateway must never send it.
//!
//! Everything else is left as it arrived: a body whose tool schemas already
//! satisfy the dialect comes back byte-for-byte, so the rewrite never churns
//! a prompt-cache prefix that was already valid, and the projection is a pure
//! function of the input — the same tools rewrite to the same bytes on every
//! turn, which is what keeps the cache hitting across requests.
//!
//! ## Where the projection runs
//!
//! Kimi serves both the Chat Completions contract (`/coding/v1/chat/
//! completions`) and the Responses contract (`/coding/v1/responses`)
//! natively, and the gateway may also translate a Responses body into chat
//! completions for it. The projection therefore walks the tool list of
//! **either** shape — the chat nesting (`function.parameters`) and the flat
//! Responses form (`parameters`) — and is applied to the outgoing request
//! body just before it is forwarded (see [`normalize_tools`]).

use serde_json::{Map, Value};

/// An upstream host that validates tool schemas with the Moonshot dialect.
///
/// Kimi is reachable under several hosts (`api.moonshot.ai`, the coding
/// endpoint `api.kimi.com`, the historical `api.moonshot.cn`); every one of
/// them runs the same stricter validator, and a base URL behind them needs
/// the projection even when the provider is configured generically
/// (`kind = "openai_compat"`).
pub fn is_moonshot_family_host(base_url: &str) -> bool {
    let lowered = base_url.to_ascii_lowercase();
    [
        "api.moonshot.ai",
        "api.moonshot.cn",
        "api.kimi.com",
        "moonshot.ai",
        "moonshot.cn",
        "kimi.com",
    ]
    .iter()
    .any(|host| lowered.contains(host))
}

const REF_KEYWORD: &str = "$ref";
const DEF_KEYWORDS: [&str; 2] = ["$defs", "definitions"];
const COMBINATORS: [&str; 3] = ["anyOf", "oneOf", "allOf"];

/// Keywords describing the document rather than the value — never merged
/// into the site of an inlined `$ref`.
const NON_VALUE_KEYWORDS: [&str; 7] = [
    "$defs",
    "definitions",
    "$schema",
    "$id",
    "$comment",
    "$anchor",
    "$recursiveAnchor",
];

/// Keywords whose value is a single subschema, and whose value is a map of
/// subschemas. The cycle cutter walks all of them: the upstream's validator
/// descends into every applicator keyword, so a recursion hidden under
/// `not` / `if` / `contains` / … is still "infinite recursion" for it and
/// must be cut here even though the projection's own dialect walk (which
/// also *types* the nodes it enters) deliberately stays on the smaller set.
const SUBSCHEMA_KEYWORDS: [&str; 11] = [
    "items",
    "additionalProperties",
    "contains",
    "if",
    "then",
    "else",
    "not",
    "unevaluatedItems",
    "unevaluatedProperties",
    "contentSchema",
    "propertyNames",
];
const SUBSCHEMA_MAP_KEYWORDS: [&str; 3] = ["properties", "patternProperties", "dependentSchemas"];

/// A bound on the reference-walk of [`has_reference_cycle`], so a
/// pathological document cannot turn the check into a hang.
const CYCLE_WALK_BUDGET: usize = 100_000;

/// How deep a `$ref` chain may be followed before the projection cuts it.
/// Honest schemas refine a reused shape two or three levels deep; the bound
/// only fences the pathological ones.
const MAX_REF_DEPTH: usize = 8;

/// Inline budget per tool schema: every inlined reference counts against it,
/// so a fan-out of refined references cannot blow the request up.
const INLINE_BUDGET: usize = 512;

/// Rewrite every tool schema of an OpenAI-shaped request body into the
/// Moonshot dialect.
///
/// Accepted shapes: the Chat Completions form (`tools[].function.parameters`)
/// and the flat Responses form (`tools[].parameters`). A body without tools,
/// a body that is not JSON, and a body whose tools already satisfy the
/// dialect all come back **byte-for-byte unchanged** — the no-op fast path is
/// what keeps an already-valid prompt-cache prefix intact.
pub fn normalize_tools(body: &[u8]) -> Vec<u8> {
    let Ok(mut root) = serde_json::from_slice::<Value>(body) else {
        return body.to_vec();
    };
    let Some(tools) = root.get_mut("tools").and_then(|t| t.as_array_mut()) else {
        return body.to_vec();
    };

    let mut changed = false;
    for tool in tools.iter_mut() {
        // The chat shape nests the schema under `function.parameters`; the
        // Responses shape keeps it flat. Only one of the two is present.
        let schema = match tool
            .get_mut("function")
            .and_then(|function| function.get_mut("parameters"))
        {
            Some(schema) => Some(schema),
            None => tool.get_mut("parameters"),
        };
        if let Some(schema) = schema {
            if project_schema(schema) {
                changed = true;
            }
            // The projection repairs the shapes it knows; a cycle that
            // survived it — hidden under a keyword its walk does not enter,
            // or closed through a plain JSON pointer outside `$defs` — is
            // the shape the upstream answers with `detected infinite
            // recursion without termination condition`. It is degraded
            // *here*, before the request leaves: the tool names survive,
            // the agent's turn continues, and no upstream round trip is
            // spent learning what a local check already knew.
            if has_reference_cycle(schema) {
                *schema = serde_json::json!({"type": "object"});
                changed = true;
            }
        }
    }
    if !changed {
        return body.to_vec();
    }
    serde_json::to_vec(&root).unwrap_or_else(|_| body.to_vec())
}

/// Degrade every tool schema to a bare `{"type":"object"}`.
///
/// The last-resort repair for a Moonshot rejection the projection did not
/// foresee: the tool names and the tool list survive (the conversation keeps
/// its function-calling surface), only the argument contracts are dropped —
/// an object accepts any JSON the model emits.
pub fn degrade_tool_schemas(body: &[u8]) -> Vec<u8> {
    let Ok(mut root) = serde_json::from_slice::<Value>(body) else {
        return body.to_vec();
    };
    let Some(tools) = root.get_mut("tools").and_then(|t| t.as_array_mut()) else {
        return body.to_vec();
    };

    for tool in tools.iter_mut() {
        let schema = match tool
            .get_mut("function")
            .and_then(|function| function.get_mut("parameters"))
        {
            Some(schema) => Some(schema),
            None => tool.get_mut("parameters"),
        };
        if let Some(schema) = schema {
            *schema = serde_json::json!({"type": "object"});
        }
    }
    serde_json::to_vec(&root).unwrap_or_else(|_| body.to_vec())
}

/// Project one tool schema into the Moonshot dialect.
///
/// Returns whether the schema changed — the caller keeps the original bytes
/// when no tool needed a rewrite.
pub fn project_schema(schema: &mut Value) -> bool {
    let before = schema.clone();
    let mut defs = project_defs(schema);
    let mut budget = INLINE_BUDGET;
    project_node(schema, &defs, 0, &mut budget, false);
    // The walk above may have kept a reference that closes a cycle (a
    // recursive shape is legal JSON Schema and an illegal Moonshot one), so
    // the reference graph is cut afterwards.
    if break_cycles(schema, &mut defs) {
        write_defs(schema, &defs);
    }
    schema != &before
}

/// The `$defs`/`definitions` entries of a schema document, projected and
/// keyed by name. A `$ref` such as `#/$defs/__schema0` resolves against this
/// map; a pointer anywhere else has no target here and counts as dangling.
fn project_defs(schema: &mut Value) -> Map<String, Value> {
    let mut defs = Map::new();
    for keyword in DEF_KEYWORDS {
        if let Some(Value::Object(entries)) = schema.get(keyword) {
            for (name, entry) in entries {
                defs.entry(name.clone()).or_insert_with(|| entry.clone());
            }
        }
    }

    // Project every entry before the value schema is walked: Moonshot
    // validates the entries themselves (`At path '$defs.__schemaN': …`), so
    // an entry that is never referenced still has to satisfy the dialect, and
    // a later inline merges keywords that are already legal. Entries resolve
    // against the document as it arrived — projection is deterministic, so an
    // entry referenced by another is projected identically wherever it is
    // inlined.
    let mut projected = Map::new();
    for (name, entry) in defs.iter() {
        let mut projected_entry = entry.clone();
        let mut budget = INLINE_BUDGET;
        project_target(&mut projected_entry, &defs, &mut Vec::new(), 0, &mut budget);
        projected.insert(name.clone(), projected_entry);
    }
    write_defs(schema, &projected);
    projected
}

/// Write the projected entries back into the schema document.
fn write_defs(schema: &mut Value, defs: &Map<String, Value>) {
    for keyword in DEF_KEYWORDS {
        if let Some(Value::Object(entries)) = schema.get_mut(keyword) {
            for (name, entry) in entries.iter_mut() {
                if let Some(fixed) = defs.get(name) {
                    *entry = fixed.clone();
                }
            }
        }
    }
}

/// Project one schema node in place.
///
/// `depth` is the number of `$ref` hops taken to reach this node and
/// `budget` the inlines still allowed for this tool schema; both fence the
/// walk against a pathological reference graph. `is_definition` marks a node
/// the document keeps in `$defs`: Moonshot validates those on their own, so
/// they are always given a type.
fn project_node(
    node: &mut Value,
    defs: &Map<String, Value>,
    depth: usize,
    budget: &mut usize,
    is_definition: bool,
) {
    let Some(object) = node.as_object_mut() else {
        return;
    };

    // ── a `$ref` ──
    // Moonshot rejects every sibling of a `$ref` ("when using $ref, type
    // should be defined in the referenced schema instead of the parent
    // schema"). A pure `{"$ref": …}` whose target is projected and typed is
    // kept as it is — that is the legal shape, and keeping it leaves an
    // already-valid schema byte-identical. Any other keyword at the site — a
    // `type`, bounds, a description — is resolved by inlining the target
    // there, the site's own keywords winning the merge.
    if let Some(reference) = object.get(REF_KEYWORD).cloned() {
        let name = reference.as_str().and_then(def_name);
        let has_siblings = object.keys().any(|keyword| {
            keyword != REF_KEYWORD && !NON_VALUE_KEYWORDS.contains(&keyword.as_str())
        });
        match name.as_ref().and_then(|name| defs.get(name)) {
            Some(target) if !has_siblings && is_typed(target) => {
                // The legal shape — the reference stays, and the node stays
                // pure: a keyword beside it would be the violation.
                return;
            }
            Some(target) if *budget > 0 => {
                *budget -= 1;
                let mut rewritten = target.clone();
                project_target(&mut rewritten, defs, &mut Vec::new(), depth + 1, budget);
                merge_site_keywords(&mut rewritten, node);
                *node = rewritten;
            }
            _ => {
                // Dangling pointer or the inline budget is spent: the
                // reference cannot survive, and the walk Moonshot runs has to
                // terminate — cut to a typed object.
                let mut cut = terminating_schema(node);
                merge_site_keywords(&mut cut, node);
                *node = cut;
            }
        }
    }

    let Some(object) = node.as_object_mut() else {
        return;
    };

    // ── a `type` beside a combinator ──
    // Moonshot answers `type should be defined in anyOf items`: the keyword
    // moves into the branches that lack one and is dropped from the parent.
    if object.contains_key("type") {
        for keyword in COMBINATORS {
            if object.get(keyword).is_some_and(|value| value.is_array()) {
                let branch_type = object.remove("type");
                if let (Some(Value::Array(branches)), Some(branch_type)) =
                    (object.get_mut(keyword), branch_type)
                {
                    for branch in branches.iter_mut() {
                        if branch.get("type").is_none() {
                            if let Some(branch_object) = branch.as_object_mut() {
                                branch_object.insert("type".into(), branch_type.clone());
                            }
                        }
                    }
                }
                break;
            }
        }
    }

    // ── a keyword for a node that names its shape nowhere ──
    // `properties` → object, `items` → array, a one-type `enum`/`const` → the
    // value type, same-typed branches → that type. A node that still names
    // nothing defaults to object — except a bare `{}`, which is the "any
    // value" schema and is left as it arrived.
    if !object.contains_key("type")
        && !COMBINATORS
            .iter()
            .any(|keyword| object.contains_key(*keyword))
    {
        if let Some(inferred) = infer_type(object) {
            object.insert("type".into(), Value::String(inferred.into()));
        } else if !object.is_empty() || is_definition {
            object.insert("type".into(), Value::String("object".into()));
        }
    }

    // ── the children walk with the same rules ──
    for keyword in ["properties", "patternProperties"] {
        if let Some(Value::Object(children)) = object.get_mut(keyword) {
            for child in children.values_mut() {
                project_node(child, defs, depth, budget, false);
            }
        }
    }
    if let Some(child) = object.get_mut("items") {
        project_node(child, defs, depth, budget, false);
    }
    if let Some(Value::Array(items)) = object.get_mut("prefixItems") {
        for item in items.iter_mut() {
            project_node(item, defs, depth, budget, false);
        }
    }
    for keyword in COMBINATORS {
        if let Some(Value::Array(branches)) = object.get_mut(keyword) {
            for branch in branches.iter_mut() {
                project_node(branch, defs, depth, budget, false);
            }
        }
    }
    if let Some(child) = object.get_mut("additionalProperties") {
        project_node(child, defs, depth, budget, false);
    }
}

/// Project a `$defs` target reached through a reference chain.
///
/// `cycle` carries the names of the defs currently being expanded, so a
/// chain that revisits one — the unbounded recursion Moonshot reports —
/// terminates instead of recursing forever.
fn project_target(
    target: &mut Value,
    defs: &Map<String, Value>,
    cycle: &mut [String],
    depth: usize,
    budget: &mut usize,
) {
    if depth > MAX_REF_DEPTH {
        *target = terminating_schema(target);
        return;
    }
    if let Some(reference) = target.as_object().and_then(|o| o.get(REF_KEYWORD)).cloned() {
        let name = reference.as_str().and_then(def_name);
        let on_cycle = name
            .as_ref()
            .is_some_and(|name| cycle.iter().any(|seen| seen == name));
        match name.as_ref().and_then(|name| defs.get(name)) {
            Some(next) if !on_cycle && *budget > 0 => {
                *budget -= 1;
                let mut rewritten = next.clone();
                let mut next_cycle = cycle.to_vec();
                next_cycle.push(name.expect("the name is resolved above").clone());
                project_target(&mut rewritten, defs, &mut next_cycle, depth + 1, budget);
                merge_site_keywords(&mut rewritten, target);
                *target = rewritten;
            }
            _ => {
                let mut cut = terminating_schema(target);
                merge_site_keywords(&mut cut, target);
                *target = cut;
            }
        }
    }
    project_node(target, defs, depth, budget, true);
}

/// Merge the keywords of `site` over `target` — the site wins — skipping the
/// reference and document keywords.
fn merge_site_keywords(target: &mut Value, site: &Value) {
    let Some(target_object) = target.as_object_mut() else {
        return;
    };
    let Some(site_object) = site.as_object() else {
        return;
    };
    for (keyword, value) in site_object {
        if keyword != REF_KEYWORD && !NON_VALUE_KEYWORDS.contains(&keyword.as_str()) {
            target_object.insert(keyword.clone(), value.clone());
        }
    }
}

/// Cut every reference that closes a cycle in the projected document.
///
/// A recursive shape (`def A → B → A`) is legal JSON Schema and an illegal
/// Moonshot one — its walk never reaches a terminating node. The cycle is
/// broken at the reference that closes it, which becomes a typed object.
///
/// Returns whether anything was cut.
fn break_cycles(schema: &mut Value, defs: &mut Map<String, Value>) -> bool {
    let mut changed = false;

    // Every definition is walked on its own: Moonshot validates the entries
    // of `$defs` even when no reference points at them.
    let names: Vec<String> = defs.keys().cloned().collect();
    for name in names {
        let mut stack = vec![name.clone()];
        let mut entry = defs.get(&name).cloned().unwrap_or(Value::Null);
        if break_cycles_in(&mut entry, defs, &mut stack) {
            defs.insert(name, entry);
            changed = true;
        }
    }

    let mut stack = Vec::new();
    if break_cycles_in(schema, defs, &mut stack) {
        changed = true;
    }
    changed
}

/// Cut the cycles reachable from one node.
///
/// `stack` carries the definitions the walk is currently inside, so a
/// reference back to one of them is the edge that closes a cycle.
fn break_cycles_in(
    node: &mut Value,
    defs: &mut Map<String, Value>,
    stack: &mut Vec<String>,
) -> bool {
    if let Some(reference) = node
        .get(REF_KEYWORD)
        .and_then(|reference| reference.as_str())
        .map(str::to_owned)
    {
        let Some(name) = def_name(&reference) else {
            return false;
        };
        if stack.iter().any(|seen| seen == &name) {
            *node = terminating_schema(node);
            return true;
        }
        if defs.contains_key(&name) {
            stack.push(name.clone());
            let mut target = defs.get(&name).cloned().unwrap_or(Value::Null);
            let changed = break_cycles_in(&mut target, defs, stack);
            if changed {
                defs.insert(name, target);
            }
            stack.pop();
            return changed;
        }
        return false;
    }

    let mut changed = false;
    let Some(object) = node.as_object_mut() else {
        return false;
    };
    for keyword in SUBSCHEMA_MAP_KEYWORDS {
        if let Some(Value::Object(children)) = object.get_mut(keyword) {
            for child in children.values_mut() {
                changed |= break_cycles_in(child, defs, stack);
            }
        }
    }
    for keyword in SUBSCHEMA_KEYWORDS {
        if let Some(child) = object.get_mut(keyword) {
            changed |= break_cycles_in(child, defs, stack);
        }
    }
    if let Some(Value::Array(items)) = object.get_mut("prefixItems") {
        for item in items.iter_mut() {
            changed |= break_cycles_in(item, defs, stack);
        }
    }
    for keyword in COMBINATORS {
        if let Some(Value::Array(branches)) = object.get_mut(keyword) {
            for branch in branches.iter_mut() {
                changed |= break_cycles_in(branch, defs, stack);
            }
        }
    }
    changed
}

/// Whether the schema still carries a reference cycle.
///
/// The projection cuts every cycle its own walk can see — but its walk is
/// bounded by the keywords it knows, and a cycle can hide under an
/// applicator keyword it does not enter (`unevaluatedProperties` on a
/// tomorrow's dialect revision, say) or close through a plain JSON pointer
/// that resolves outside `$defs`. A cycle is the one shape the upstream
/// answers with `detected infinite recursion without termination
/// condition`, and the one shape that ends an agent's turn — so before a
/// projected schema is forwarded, the reference graph is re-checked and a
/// surviving cycle is degraded here instead of being sent.
///
/// The walk follows **any** `$ref` — not only `#/$defs/…` pointers — and
/// enters every subschema keyword, so it sees exactly what the upstream's
/// validator sees. A document too large to prove safe within the budget is
/// reported as cyclic: the schema is degraded, and a degraded schema is
/// always legal.
pub fn has_reference_cycle(schema: &Value) -> bool {
    let mut budget = CYCLE_WALK_BUDGET;
    let mut stack = Vec::new();
    walk_for_cycle(schema, schema, &mut stack, &mut budget)
}

/// Walk one node of the reference graph looking for a cycle.
///
/// `doc` is the schema document the pointers resolve against, `stack` the
/// pointer paths of the reference targets currently being expanded — a
/// reference back to one of them is the edge that closes a cycle. The
/// document itself is a tree, so every cycle must cross at least one `$ref`;
/// entering every child value (any keyword, any nesting) is what makes the
/// check complete.
fn walk_for_cycle(node: &Value, doc: &Value, stack: &mut Vec<String>, budget: &mut usize) -> bool {
    if *budget == 0 {
        // Too large to prove safe within the budget: assume the worst. A
        // degraded schema is legal; a missed cycle is a killed turn.
        return true;
    }
    *budget -= 1;

    if let Some(pointer) = node.get(REF_KEYWORD).and_then(Value::as_str) {
        if let Some((path, target)) = resolve_pointer(doc, pointer) {
            if stack.iter().any(|seen| seen == &path) {
                return true;
            }
            stack.push(path);
            if walk_for_cycle(target, doc, stack, budget) {
                return true;
            }
            stack.pop();
        }
    }

    match node {
        Value::Object(object) => {
            for (keyword, value) in object {
                if keyword == REF_KEYWORD {
                    // Followed above; a value under the keyword itself is
                    // the pointer string, never a subschema.
                    continue;
                }
                if walk_for_cycle(value, doc, stack, budget) {
                    return true;
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                if walk_for_cycle(item, doc, stack, budget) {
                    return true;
                }
            }
        }
        _ => {}
    }
    false
}

/// Resolve a JSON pointer inside the schema document.
///
/// Returns the canonical path of the target — rebuilt from the tokens the
/// pointer walks, so two spellings that reach the same node name it
/// identically — and the target itself. Only pointers rooted at the document
/// resolve here; a pointer into another document (a `$id`, say) is unknown
/// and yields nothing, which the caller reads as "cannot cycle".
fn resolve_pointer<'a>(doc: &'a Value, pointer: &str) -> Option<(String, &'a Value)> {
    let rest = pointer.strip_prefix('#')?;
    let mut node = doc;
    let mut path = String::from("#");
    for token in rest.split('/').skip_while(|token| token.is_empty()) {
        let token = token.replace("~1", "/").replace("~0", "~");
        node = match node {
            Value::Object(map) => {
                path.push_str("/o:");
                map.get(&token)?
            }
            Value::Array(items) => {
                path.push_str("/i:");
                items.get(token.parse::<usize>().ok()?)?
            }
            _ => return None,
        };
        path.push_str(&token);
    }
    Some((path, node))
}

/// The schema a cut reference chain terminates with.
///
/// An object is the honest bound for a recursive value: the agent's tool
/// argument keeps its shape one level deeper than the walk went, and the
/// upstream sees a typed, terminating schema.
fn terminating_schema(site: &Value) -> Value {
    let mut terminating = Map::new();
    terminating.insert("type".into(), Value::String("object".into()));
    if let Some(description) = site.get("description") {
        terminating.insert("description".into(), description.clone());
    }
    Value::Object(terminating)
}

/// Whether a projected target names its type (or is a combinator, which
/// names it in the branches).
fn is_typed(target: &Value) -> bool {
    let Some(object) = target.as_object() else {
        return false;
    };
    object.contains_key("type")
        || COMBINATORS
            .iter()
            .any(|keyword| object.contains_key(*keyword))
}

/// The def name of a local pointer (`#/$defs/name`, `#/definitions/name`).
fn def_name(pointer: &str) -> Option<String> {
    let pointer = pointer.strip_prefix('#')?;
    let mut segments = pointer.split('/').filter(|segment| !segment.is_empty());
    let keyword = segments.next()?;
    if !DEF_KEYWORDS.contains(&keyword) {
        return None;
    }
    let name = segments.next()?;
    // JSON Pointer escapes: `~0` is `~`, `~1` is `/`.
    Some(name.replace("~1", "/").replace("~0", "~"))
}

/// The keyword a schema's own shape implies.
fn infer_type(object: &Map<String, Value>) -> Option<&'static str> {
    if object.contains_key("properties")
        || object.contains_key("patternProperties")
        || object.contains_key("required")
    {
        return Some("object");
    }
    if object.contains_key("items") || object.contains_key("prefixItems") {
        return Some("array");
    }
    if let Some(values) = object.get("enum").and_then(|e| e.as_array()) {
        return enum_value_type(values);
    }
    if let Some(value) = object.get("const") {
        return json_type(value);
    }
    // Every branch carrying the same keyword makes the whole node that type —
    // the `z.string().nullable()` shape whose target is an `anyOf` of two
    // `string` branches is exactly that case.
    for keyword in ["anyOf", "oneOf"] {
        if let Some(Value::Array(branches)) = object.get(keyword) {
            let mut common: Option<&'static str> = None;
            for branch in branches {
                let branch_type = static_json_type(branch.get("type").and_then(|t| t.as_str())?)?;
                match common {
                    None => common = Some(branch_type),
                    Some(shared) if shared == branch_type => {}
                    _ => return None,
                }
            }
            return common;
        }
    }
    None
}

/// The canonical schema keyword behind a raw string, if it is one.
fn static_json_type(keyword: &str) -> Option<&'static str> {
    match keyword {
        "object" => Some("object"),
        "array" => Some("array"),
        "string" => Some("string"),
        "number" => Some("number"),
        "integer" => Some("integer"),
        "boolean" => Some("boolean"),
        "null" => Some("null"),
        _ => None,
    }
}

/// The schema keyword for a JSON value.
fn json_type(value: &Value) -> Option<&'static str> {
    match value {
        Value::Null => Some("null"),
        Value::Bool(_) => Some("boolean"),
        Value::Number(_) => Some("number"),
        Value::String(_) => Some("string"),
        Value::Array(_) => Some("array"),
        Value::Object(_) => Some("object"),
    }
}

/// The one type every member of an `enum` shares, if there is one.
fn enum_value_type(values: &[Value]) -> Option<&'static str> {
    let mut common: Option<&'static str> = None;
    for value in values {
        let value_type = json_type(value)?;
        match common {
            None => common = Some(value_type),
            Some(shared) if shared == value_type => {}
            _ => return None,
        }
    }
    common
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeSet;

    // ── the Moonshot-rules validator ──────────────────────────────────
    //
    // Every projected schema must satisfy the dialect the upstream enforces:
    //
    // 1. a `$ref` never carries a sibling value keyword;
    // 2. every `$ref` resolves inside the document to a node that names its
    //    type (or is a combinator naming it in the branches);
    // 3. no reference cycle survives — the walk the upstream runs must end;
    // 4. every node that is neither a reference nor a combinator names its
    //    type, except a bare `{}` ("any value") outside `$defs`;
    // 5. every `$defs` entry names its type.

    fn assert_moonshot_legal(schema: &Value) {
        let defs = collect_defs(schema);
        let mut visiting = BTreeSet::new();
        check_node(schema, &defs, &mut visiting, true, "root");
        for (name, def) in &defs {
            let mut visiting = BTreeSet::new();
            visiting.insert(name.clone());
            check_node(def, &defs, &mut visiting, true, name);
        }
    }

    fn collect_defs(schema: &Value) -> Map<String, Value> {
        let mut defs = Map::new();
        for keyword in DEF_KEYWORDS {
            if let Some(Value::Object(entries)) = schema.get(keyword) {
                for (name, entry) in entries {
                    defs.insert(name.clone(), entry.clone());
                }
            }
        }
        defs
    }

    fn check_node(
        node: &Value,
        defs: &Map<String, Value>,
        visiting: &mut BTreeSet<String>,
        inside_defs: bool,
        path: &str,
    ) {
        let Some(object) = node.as_object() else {
            return;
        };

        if let Some(reference) = object.get(REF_KEYWORD) {
            let pointer = reference.as_str().unwrap_or_default();
            // rule 1 — a pure reference site.
            for keyword in object.keys() {
                if keyword != REF_KEYWORD && !NON_VALUE_KEYWORDS.contains(&keyword.as_str()) {
                    panic!("{path}: a `$ref` carries the sibling `{keyword}`: {node}");
                }
            }
            // rule 2 — the target names its type.
            let name = def_name(pointer).unwrap_or_else(|| panic!("{path}: bad pointer {pointer}"));
            let target = defs
                .get(&name)
                .unwrap_or_else(|| panic!("{path}: dangling reference {pointer}"));
            if !is_typed(target) {
                panic!("{path}: reference target `{name}` names no type: {target}");
            }
            // rule 3 — no cycle.
            if !visiting.insert(name.clone()) {
                panic!("{path}: reference cycle through `{name}`: {node}");
            }
            check_node(target, defs, visiting, true, &format!("{path} → {name}"));
            visiting.remove(&name);
            return;
        }

        // rule 4/5 — a type (or a combinator) on every node.
        let is_combinator = COMBINATORS
            .iter()
            .any(|keyword| object.contains_key(*keyword));
        if !object.contains_key("type") && !is_combinator {
            if !inside_defs && object.is_empty() {
                // a bare `{}` is the "any value" schema — allowed outside defs
            } else {
                panic!("{path}: node names no type: {node}");
            }
        }

        for keyword in ["properties", "patternProperties"] {
            if let Some(Value::Object(children)) = object.get(keyword) {
                for (name, child) in children {
                    check_node(child, defs, visiting, false, &format!("{path}.{name}"));
                }
            }
        }
        if let Some(child) = object.get("items") {
            check_node(child, defs, visiting, false, &format!("{path}.items"));
        }
        if let Some(Value::Array(items)) = object.get("prefixItems") {
            for (i, item) in items.iter().enumerate() {
                check_node(item, defs, visiting, false, &format!("{path}[{i}]"));
            }
        }
        for keyword in COMBINATORS {
            if let Some(Value::Array(branches)) = object.get(keyword) {
                for (i, branch) in branches.iter().enumerate() {
                    check_node(
                        branch,
                        defs,
                        visiting,
                        false,
                        &format!("{path}.{keyword}[{i}]"),
                    );
                }
            }
        }
        if let Some(child) = object.get("additionalProperties") {
            check_node(
                child,
                defs,
                visiting,
                false,
                &format!("{path}.additionalProperties"),
            );
        }
    }

    fn project(schema: Value) -> Value {
        let mut schema = schema;
        let changed = project_schema(&mut schema);
        assert!(changed, "the schema was expected to need a rewrite");
        assert_moonshot_legal(&schema);
        schema
    }

    // ── the reported error ─────────────────────────────────────────────

    #[test]
    fn the_reported_shape_projects_to_a_terminating_schema() {
        // `At path 'properties.show': detected infinite recursion without
        // termination condition` — `show` resolves through a def that points
        // back at itself.
        let schema = project(json!({
            "type": "object",
            "properties": {
                "show": {"$ref": "#/$defs/__schema0"}
            },
            "$defs": {
                "__schema0": {"$ref": "#/$defs/__schema0"}
            }
        }));
        // The reference survives only if its target is typed and the chain
        // terminates — both are what the validator above just checked.
        assert_eq!(schema["$defs"]["__schema0"], json!({"type": "object"}));
        assert_eq!(
            schema["properties"]["show"],
            json!({"$ref": "#/$defs/__schema0"}),
            "a pure reference to the cut target is the honest shape"
        );
    }

    #[test]
    fn the_reported_tool_body_projects_and_stays_deterministic() {
        let body = br##"{"model":"kimi-for-coding","input":"hi","tools":[
            {"type":"function","name":"memory_feedback","parameters":{
                "type":"object",
                "properties":{"show":{"$ref":"#/$defs/__schema0"}},
                "required":["show"],
                "$defs":{"__schema0":{"$ref":"#/$defs/__schema0"}}
            }}]}"##;

        let once = normalize_tools(body);
        let twice = normalize_tools(&once);
        assert_eq!(once, twice, "the projection must be idempotent");

        let out: Value = serde_json::from_slice(&once).unwrap();
        assert_moonshot_legal(&out["tools"][0]["parameters"]);
        assert_eq!(out["tools"][0]["parameters"]["type"], "object");
        assert_eq!(out["tools"][0]["parameters"]["required"], json!(["show"]));
    }

    // ── the reported `properties.source` shape ─────────────────────────

    #[test]
    fn the_reported_source_shape_projects_to_a_terminating_schema() {
        // `At path 'properties.source': detected infinite recursion without
        // termination condition` — the same zod shape as the `show` one,
        // under the property name the upstream named this time.
        let schema = project(json!({
            "type": "object",
            "properties": {
                "source": {"$ref": "#/$defs/__schema0"}
            },
            "$defs": {
                "__schema0": {"$ref": "#/$defs/__schema0"}
            }
        }));
        assert_eq!(schema["$defs"]["__schema0"], json!({"type": "object"}));
        assert_eq!(
            schema["properties"]["source"],
            json!({"$ref": "#/$defs/__schema0"}),
            "a pure reference to the cut target is the honest shape"
        );
        assert!(!has_reference_cycle(&schema));
    }

    #[test]
    fn a_recursive_source_property_is_cut() {
        // The property recurses into its own type: `source` is a node of the
        // type it points at.
        let schema = project(json!({
            "type": "object",
            "properties": {
                "source": {"$ref": "#/$defs/__schema0"}
            },
            "$defs": {
                "__schema0": {
                    "type": "object",
                    "properties": {
                        "source": {"$ref": "#/$defs/__schema0"}
                    }
                }
            }
        }));
        assert_eq!(schema["$defs"]["__schema0"]["type"], "object");
        assert_eq!(
            schema["$defs"]["__schema0"]["properties"]["source"],
            json!({"type": "object"}),
            "the back edge is cut at a terminating object"
        );
        assert!(!has_reference_cycle(&schema));
    }

    #[test]
    fn a_cycle_under_an_applicator_keyword_is_cut() {
        // The recursion hides under `not` — a keyword the projection's
        // dialect walk does not enter. The cutter does, so the schema the
        // upstream sees still terminates.
        let schema = project(json!({
            "type": "object",
            "properties": {
                "source": {
                    "type": "object",
                    "not": {"$ref": "#/$defs/__schema0"}
                }
            },
            "$defs": {
                "__schema0": {
                    "type": "object",
                    "not": {"$ref": "#/$defs/__schema0"}
                }
            }
        }));
        assert_eq!(
            schema["$defs"]["__schema0"]["not"],
            json!({"type": "object"}),
            "the hidden back edge is cut too"
        );
        assert!(!has_reference_cycle(&schema));
    }

    #[test]
    fn a_cycle_the_projection_cannot_reach_is_degraded_before_it_leaves() {
        // The recursion closes through a plain JSON pointer —
        // `#/properties/source` — under a keyword the projection does not
        // walk. Nothing in the projection can cut it, and the upstream
        // would answer `detected infinite recursion without termination
        // condition`: a request-fault 400 that ends the agent's turn. The
        // schema is degraded here instead, before the request leaves.
        let body = br##"{"model":"m","input":"hi","tools":[
            {"type":"function","name":"t","parameters":{
                "type":"object",
                "properties":{
                    "source":{"type":"object","not":{"$ref":"#/properties/source"}}
                }
            }}]}"##;

        let out: Value = serde_json::from_slice(&normalize_tools(body)).unwrap();
        assert_eq!(
            out["tools"][0]["parameters"],
            json!({"type": "object"}),
            "a cycle that cannot be cut degrades the schema, not the turn"
        );
        assert_eq!(out["tools"][0]["name"], "t", "the tool itself survives");

        // The projection alone neither cuts nor flags it: the pointer
        // resolves outside `$defs` and sits under a keyword the dialect
        // walk does not enter, so the schema is handed on exactly as it
        // arrived — cycle and all. Only the local check sees it, which is
        // why the check is not optional.
        let mut projected = json!({
            "type": "object",
            "properties": {
                "source": {"type": "object", "not": {"$ref": "#/properties/source"}}
            }
        });
        project_schema(&mut projected);
        assert!(
            has_reference_cycle(&projected),
            "the pointer cycle survives the projection — the local check is what catches it"
        );
    }

    #[test]
    fn the_cycle_check_sees_every_recursion_and_no_other_shape() {
        // A cycle through `$defs`.
        assert!(has_reference_cycle(&json!({
            "type": "object",
            "properties": {"source": {"$ref": "#/$defs/__schema0"}},
            "$defs": {"__schema0": {"$ref": "#/$defs/__schema0"}}
        })));
        // A cycle through a plain JSON pointer.
        assert!(has_reference_cycle(&json!({
            "type": "object",
            "properties": {"source": {"$ref": "#/properties/source"}}
        })));
        // A cycle back to the document root.
        assert!(has_reference_cycle(&json!({
            "type": "object",
            "properties": {"source": {"$ref": "#"}}
        })));
        // A mutual recursion.
        assert!(has_reference_cycle(&json!({
            "type": "object",
            "properties": {"a": {"$ref": "#/$defs/__a"}},
            "$defs": {
                "__a": {"properties": {"b": {"$ref": "#/$defs/__b"}}},
                "__b": {"properties": {"a": {"$ref": "#/$defs/__a"}}}
            }
        })));
        // A legal schema is not a cycle.
        assert!(!has_reference_cycle(&json!({
            "type": "object",
            "properties": {"source": {"type": "string"}},
            "$defs": {"__schema0": {"type": "object"}}
        })));
        // A reference that resolves to a typed node terminates.
        assert!(!has_reference_cycle(&json!({
            "type": "object",
            "properties": {"source": {"$ref": "#/$defs/__schema0"}},
            "$defs": {"__schema0": {"type": "string"}}
        })));
    }

    // ── the zod shapes ────────────────────────────────────────────────

    #[test]
    fn a_ref_beside_a_refined_type_inlines_the_target_under_the_site_type() {
        let schema = project(json!({
            "type": "object",
            "properties": {
                "score": {
                    "type": "integer",
                    "maximum": 9007199254740991i64,
                    "$ref": "#/$defs/__schema0"
                }
            },
            "$defs": {"__schema0": {"type": "number", "minimum": 0}}
        }));

        let score = &schema["properties"]["score"];
        assert_eq!(score["type"], "integer", "the site's type wins: {score}");
        assert_eq!(
            score["minimum"], 0,
            "the target's bounds are merged in: {score}"
        );
        assert_eq!(score["maximum"], 9007199254740991i64);
        assert!(score.get("$ref").is_none());
    }

    #[test]
    fn a_pure_ref_to_a_typed_def_is_left_alone() {
        let mut schema = json!({
            "type": "object",
            "properties": {"kind": {"$ref": "#/$defs/Kind"}},
            "$defs": {"Kind": {"type": "string", "enum": ["helpful", "not_helpful"]}}
        });
        let before = schema.clone();
        assert!(!project_schema(&mut schema));
        assert_eq!(schema, before);
    }

    #[test]
    fn a_ref_to_an_untyped_def_gains_the_inferred_type() {
        let schema = project(json!({
            "type": "object",
            "properties": {"meta": {"$ref": "#/$defs/__schema1"}},
            "$defs": {"__schema1": {"properties": {"a": {"type": "string"}}}}
        }));
        assert_eq!(
            schema["$defs"]["__schema1"]["type"], "object",
            "the def is typed where the validator walks: {schema}"
        );
        assert_eq!(
            schema["properties"]["meta"],
            json!({"$ref": "#/$defs/__schema1"}),
            "the pure reference stays"
        );
    }

    #[test]
    fn a_ref_to_an_any_of_of_string_branches_stays_a_legal_combinator() {
        // A combinator names its type in the branches — the reference and
        // the def are already the legal shape, so nothing is rewritten.
        let mut schema = json!({
            "type": "object",
            "properties": {"choice": {"$ref": "#/$defs/__schema2"}},
            "$defs": {
                "__schema2": {"anyOf": [
                    {"type": "string", "const": "helpful"},
                    {"type": "string", "const": "not_helpful"}
                ]}
            }
        });
        let before = schema.clone();
        assert!(!project_schema(&mut schema));
        assert_eq!(schema, before);
        assert_moonshot_legal(&schema);
        assert_eq!(schema["$defs"]["__schema2"]["anyOf"][0]["type"], "string");
    }

    #[test]
    fn a_dangling_ref_collapses_instead_of_reaching_the_upstream() {
        let schema = project(json!({
            "type": "object",
            "properties": {"kind": {"$ref": "#/$defs/Ghost"}}
        }));
        let kind = &schema["properties"]["kind"];
        assert_eq!(kind["type"], "object");
        assert!(kind.get("$ref").is_none());
    }

    // ── the combinator and value shapes ───────────────────────────────

    #[test]
    fn a_type_beside_any_of_moves_into_the_branches() {
        let schema = project(json!({
            "type": "object",
            "properties": {
                "value": {
                    "type": "string",
                    "anyOf": [
                        {"enum": ["a", "b"]},
                        {"maxLength": 3}
                    ]
                }
            }
        }));
        let value = &schema["properties"]["value"];
        assert!(
            value.get("type").is_none(),
            "the parent type is gone: {value}"
        );
        assert_eq!(value["anyOf"][0]["type"], "string");
        assert_eq!(value["anyOf"][1]["type"], "string");
    }

    #[test]
    fn an_enum_without_a_type_gains_the_shared_value_type() {
        let schema = project(json!({
            "type": "object",
            "properties": {"mode": {"enum": ["auto", "manual"]}}
        }));
        assert_eq!(schema["properties"]["mode"]["type"], "string");
    }

    #[test]
    fn a_const_without_a_type_gains_the_value_type() {
        let schema = project(json!({
            "type": "object",
            "properties": {"version": {"const": 2}}
        }));
        assert_eq!(schema["properties"]["version"]["type"], "number");
    }

    #[test]
    fn a_bare_object_without_a_type_is_named_object() {
        let schema = project(json!({
            "properties": {"a": {"type": "string"}}
        }));
        assert_eq!(schema["type"], "object");
    }

    #[test]
    fn a_bare_any_schema_stays_the_any_schema() {
        // A bare `{}` is the "any value" schema: naming a type for it would
        // misstate the contract, and Moonshot accepts it as it is.
        let mut schema = json!({
            "type": "object",
            "properties": {"anything": {}}
        });
        let before = schema.clone();
        assert!(!project_schema(&mut schema));
        assert_eq!(schema, before);
        assert_moonshot_legal(&schema);
        assert_eq!(schema["properties"]["anything"], json!({}));
    }

    // ── recursion ─────────────────────────────────────────────────────

    #[test]
    fn a_mutually_recursive_pair_is_cut() {
        // defs A → B → A: honest recursion (a JSON tree), and exactly the
        // chain Moonshot calls unbounded.
        let schema = project(json!({
            "type": "object",
            "properties": {"node": {"$ref": "#/$defs/__a"}},
            "$defs": {
                "__a": {
                    "type": "object",
                    "properties": {"child": {"$ref": "#/$defs/__b"}}
                },
                "__b": {
                    "type": "object",
                    "properties": {"child": {"$ref": "#/$defs/__a"}}
                }
            }
        }));
        // One hop of structure is preserved before the cut.
        assert_eq!(schema["$defs"]["__a"]["type"], "object");
        let child = &schema["$defs"]["__b"]["properties"]["child"];
        assert_eq!(child["type"], "object", "the closing edge is cut: {schema}");
        assert!(child.get("$ref").is_none());
    }

    // ── normalize_tools: the two wire shapes ─────────────────────────

    #[test]
    fn a_chat_shaped_tool_list_is_projected() {
        let body = br##"{"model":"m","messages":[],"tools":[
            {"type":"function","function":{
                "name":"show","parameters":{
                    "type":"object",
                    "properties":{"show":{"$ref":"#/$defs/__schema0"}},
                    "$defs":{"__schema0":{"$ref":"#/$defs/__schema0"}}
                }}}]}"##;
        let out: Value = serde_json::from_slice(&normalize_tools(body)).unwrap();
        assert_moonshot_legal(&out["tools"][0]["function"]["parameters"]);
    }

    #[test]
    fn a_flat_responses_tool_list_is_projected() {
        let body = br##"{"model":"m","input":"hi","tools":[
            {"type":"function","name":"show","parameters":{
                "type":"object",
                "properties":{"show":{"$ref":"#/$defs/__schema0"}},
                "$defs":{"__schema0":{"$ref":"#/$defs/__schema0"}}
            }}]}"##;
        let out: Value = serde_json::from_slice(&normalize_tools(body)).unwrap();
        assert_moonshot_legal(&out["tools"][0]["parameters"]);
    }

    #[test]
    fn a_clean_body_stays_byte_for_byte() {
        let body = br#"{"model":"m","input":"hi","stream":true,"tools":[
            {"type":"function","name":"f","parameters":{"type":"object","properties":{"a":{"type":"string"}}}}
        ]}"#;
        assert_eq!(normalize_tools(body), body.to_vec());

        assert_eq!(normalize_tools(b"not json"), b"not json".to_vec());
        assert_eq!(
            normalize_tools(br#"{"model":"m","input":"hi"}"#),
            br#"{"model":"m","input":"hi"}"#.to_vec()
        );
        assert_eq!(
            normalize_tools(br#"{"tools":[]}"#),
            br#"{"tools":[]}"#.to_vec()
        );
    }

    #[test]
    fn degrade_replaces_every_parameter_schema_with_a_bare_object() {
        let body = br#"{"model":"m","tools":[
            {"type":"function","name":"a","parameters":{"type":"object","properties":{"x":{"type":"string"}}}},
            {"type":"function","name":"b","parameters":{"type":"string"}}
        ]}"#;
        let out: Value = serde_json::from_slice(&degrade_tool_schemas(body)).unwrap();
        let tools = out["tools"].as_array().unwrap();
        for tool in tools {
            assert_eq!(tool["parameters"], json!({"type": "object"}), "{tool}");
        }
        assert_eq!(tools[0]["name"], "a");
        assert_eq!(tools[1]["name"], "b");
    }

    #[test]
    fn degrade_keeps_a_body_without_tools_unchanged() {
        let body = br#"{"model":"m","input":"hi"}"#;
        assert_eq!(degrade_tool_schemas(body), body.to_vec());
        assert_eq!(degrade_tool_schemas(b"nope"), b"nope".to_vec());
    }

    // ── host detection ───────────────────────────────────────────────

    #[test]
    fn moonshot_family_hosts_are_detected() {
        for url in [
            "https://api.moonshot.ai/v1",
            "https://api.moonshot.cn/v1",
            "https://api.kimi.com/coding/v1",
            "https://moonshot.ai/v1",
        ] {
            assert!(is_moonshot_family_host(url), "{url}");
        }
        assert!(!is_moonshot_family_host("https://api.openai.com/v1"));
        assert!(!is_moonshot_family_host("https://api.deepseek.com"));
        assert!(!is_moonshot_family_host(
            "https://ark.cn-beijing.volces.com/api/v3"
        ));
        assert!(!is_moonshot_family_host("http://127.0.0.1:11434"));
    }

    // ── the walk fences itself ───────────────────────────────────────

    #[test]
    fn a_deeper_than_max_chain_terminates() {
        // __0 → __1 → … → __9 → __0 is longer than MAX_REF_DEPTH either way;
        // the walk must return a terminating schema, never hang or overflow.
        let mut schema = json!({"type": "object", "properties": {"deep": {"$ref": "#/$defs/__0"}}});
        let mut defs = Map::new();
        for i in 0..10 {
            let next = format!("#/$defs/__{}", (i + 1) % 10);
            defs.insert(format!("__{i}"), json!({"$ref": next}));
        }
        schema["$defs"] = Value::Object(defs);

        assert!(project_schema(&mut schema));
        assert_moonshot_legal(&schema);
    }

    #[test]
    fn a_fan_out_of_forced_inlines_stays_bounded() {
        // Every property references a def that references two more, with a
        // sibling keyword at each site so the reference cannot be kept —
        // exactly the shape that can blow up a naive inliner. The budget
        // caps the walk.
        let mut properties = Map::new();
        for i in 0..12 {
            properties.insert(
                format!("f{i}"),
                json!({"description": "force the inline", "$ref": "#/$defs/__base"}),
            );
        }
        let mut schema = json!({
            "type": "object",
            "properties": properties,
            "$defs": {
                "__base": {
                    "type": "object",
                    "properties": {
                        "a": {"$ref": "#/$defs/__leaf"},
                        "b": {"$ref": "#/$defs/__leaf"}
                    }
                },
                "__leaf": {"type": "string"}
            }
        });
        let before_len = serde_json::to_string(&schema).unwrap().len();
        assert!(project_schema(&mut schema));
        assert_moonshot_legal(&schema);
        let after_len = serde_json::to_string(&schema).unwrap().len();
        assert!(
            after_len < 20_000,
            "the inline budget must cap the expansion ({before_len} → {after_len})"
        );
        assert!(after_len > before_len);
    }
}

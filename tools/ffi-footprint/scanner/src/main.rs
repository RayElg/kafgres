//! Extracts FFI-relevant facts from a Rust crate's source as JSON: functions and their
//! attributes and ABI, every call site with its enclosing argument positions, unsafe
//! blocks and the operations inside them, panic sites, and error-raising macros.
//!
//! Call resolution, handler inference and entry classification happen in `report.py`.
//!
//! Usage: ffi-footprint-scanner --src <crate dir> [--features a,b] [--cfg k=v,...]
//!        [--config conf.json] > facts.json

use serde_json::{json, Map, Value};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Attribute, Expr, Item, Meta, Token};

struct Conf {
    ffi_modules: HashSet<String>,
    unsafe_apis: HashSet<String>,
    unsafe_methods: HashSet<String>,
    panic_methods: HashSet<String>,
    panic_macros: HashSet<String>,
    raise_macros: HashMap<String, String>,
    macro_ffi: HashMap<String, String>,
    closure_entries: HashSet<String>,
}

fn strings(v: &Value, key: &str, default: &[&str]) -> HashSet<String> {
    match v.get(key).and_then(Value::as_array) {
        Some(a) => a.iter().filter_map(|s| s.as_str().map(String::from)).collect(),
        None => default.iter().map(|s| s.to_string()).collect(),
    }
}

fn string_map(v: &Value, key: &str, default: &[(&str, &str)]) -> HashMap<String, String> {
    match v.get(key).and_then(Value::as_object) {
        Some(o) => o
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect(),
        None => default.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
    }
}

impl Conf {
    fn from_json(v: &Value) -> Conf {
        Conf {
            ffi_modules: strings(v, "ffi_modules", &["pg_sys"]),
            unsafe_apis: strings(
                v,
                "unsafe_apis",
                &[
                    "from_raw_parts", "from_raw_parts_mut", "transmute", "transmute_copy",
                    "zeroed", "uninitialized", "from_utf8_unchecked", "from_ptr", "from_raw",
                    "read", "write", "read_unaligned", "write_unaligned", "read_volatile",
                    "write_volatile", "copy", "copy_nonoverlapping", "write_bytes",
                    "drop_in_place", "new_unchecked", "from_raw_fd", "unreachable_unchecked",
                    "from_bytes_with_nul_unchecked",
                ],
            ),
            unsafe_methods: strings(
                v,
                "unsafe_methods",
                &[
                    "get_unchecked", "get_unchecked_mut", "offset", "add", "sub", "read",
                    "write", "read_unaligned", "write_unaligned", "as_ref", "as_mut",
                    "assume_init", "assume_init_ref", "set_len", "copy_to",
                    "copy_to_nonoverlapping", "copy_from", "copy_from_nonoverlapping",
                    "offset_from", "byte_add", "cast_mut",
                ],
            ),
            panic_methods: strings(v, "panic_methods", &["unwrap", "expect", "unwrap_err", "expect_err"]),
            panic_macros: strings(
                v,
                "panic_macros",
                &["panic", "unreachable", "todo", "unimplemented", "assert", "assert_eq", "assert_ne"],
            ),
            raise_macros: string_map(
                v,
                "raise_macros",
                &[("error", "ERROR"), ("FATAL", "FATAL"), ("PANIC", "PANIC"), ("ereport", "dynamic")],
            ),
            macro_ffi: string_map(v, "macro_ffi", &[("check_for_interrupts", "ProcessInterrupts")]),
            closure_entries: strings(v, "closure_entries", &["register_xact_callback", "register_subxact_callback"]),
        }
    }
}

struct Cfg {
    features: HashSet<String>,
    set: HashSet<String>,
    unknown: BTreeSet<String>,
}

const KNOWN_KEYS: &[&str] = &[
    "target_os", "target_family", "target_arch", "target_pointer_width", "target_endian",
    "target_env", "target_vendor", "panic", "target_has_atomic", "target_feature",
];
const KNOWN_FLAGS: &[&str] = &["test", "debug_assertions", "doc", "doctest", "miri", "unix", "windows"];

impl Cfg {
    fn eval(&mut self, m: &Meta) -> bool {
        match m {
            Meta::Path(p) => {
                let n = path_str(p);
                if self.set.contains(&n) {
                    true
                } else if KNOWN_FLAGS.contains(&n.as_str()) {
                    false
                } else {
                    self.unknown.insert(n);
                    true
                }
            }
            Meta::NameValue(nv) => {
                let k = path_str(&nv.path);
                let v = expr_lit_str(&nv.value).unwrap_or_default();
                if k == "feature" {
                    return self.features.contains(&v);
                }
                let key = format!("{k}={v}");
                if self.set.contains(&key) {
                    true
                } else if KNOWN_KEYS.contains(&k.as_str()) {
                    false
                } else {
                    self.unknown.insert(key);
                    true
                }
            }
            Meta::List(l) => {
                let name = path_str(&l.path);
                let nested: Punctuated<Meta, Token![,]> =
                    match l.parse_args_with(Punctuated::parse_terminated) {
                        Ok(n) => n,
                        Err(_) => {
                            self.unknown.insert(name);
                            return true;
                        }
                    };
                let vals: Vec<bool> = nested.iter().map(|m| self.eval(m)).collect();
                match name.as_str() {
                    "all" => vals.iter().all(|b| *b),
                    "any" => vals.iter().any(|b| *b),
                    "not" => !vals.first().copied().unwrap_or(false),
                    _ => {
                        self.unknown.insert(name);
                        true
                    }
                }
            }
        }
    }

    fn enabled(&mut self, attrs: &[Attribute]) -> bool {
        for a in attrs {
            if a.path().is_ident("cfg") {
                match a.parse_args::<Meta>() {
                    Ok(m) => {
                        if !self.eval(&m) {
                            return false;
                        }
                    }
                    Err(_) => {
                        self.unknown.insert("<unparsed cfg>".into());
                    }
                }
            }
        }
        true
    }
}

fn path_str(p: &syn::Path) -> String {
    p.segments.iter().map(|s| s.ident.to_string()).collect::<Vec<_>>().join("::")
}

fn path_segs(p: &syn::Path) -> Vec<String> {
    p.segments.iter().map(|s| s.ident.to_string()).collect()
}

fn expr_lit_str(e: &Expr) -> Option<String> {
    if let Expr::Lit(l) = e {
        if let syn::Lit::Str(s) = &l.lit {
            return Some(s.value());
        }
    }
    None
}

fn line_of<T: Spanned>(t: &T) -> usize {
    t.span().start().line
}

fn end_line_of<T: Spanned>(t: &T) -> usize {
    t.span().end().line
}

fn type_name(t: &syn::Type) -> String {
    match t {
        syn::Type::Path(p) => p.path.segments.last().map(|s| s.ident.to_string()).unwrap_or_default(),
        syn::Type::Reference(r) => type_name(&r.elem),
        other => quote_tokens(other),
    }
}

fn quote_tokens<T: quote::ToTokens>(t: &T) -> String {
    t.to_token_stream().to_string()
}

fn is_raw_ptr_type(t: &syn::Type) -> bool {
    matches!(t, syn::Type::Ptr(_))
}

/// Attribute names that matter for FFI, with pgrx paths and `unsafe(...)` unwrapped.
fn attr_facts(attrs: &[Attribute]) -> (Vec<String>, Option<String>, bool) {
    let mut names = Vec::new();
    let mut export_name = None;
    let mut safety_doc = false;
    for a in attrs {
        let last = a.path().segments.last().map(|s| s.ident.to_string()).unwrap_or_default();
        match last.as_str() {
            "doc" => {
                if let Meta::NameValue(nv) = &a.meta {
                    if let Some(s) = expr_lit_str(&nv.value) {
                        let t = s.trim();
                        if t.starts_with("# Safety") || t.starts_with("SAFETY") {
                            safety_doc = true;
                        }
                    }
                }
            }
            "unsafe" => {
                if let Ok(m) = a.parse_args::<Meta>() {
                    let inner = m.path().segments.last().map(|s| s.ident.to_string()).unwrap_or_default();
                    if inner == "export_name" {
                        if let Meta::NameValue(nv) = &m {
                            export_name = expr_lit_str(&nv.value);
                        }
                    }
                    names.push(inner);
                }
            }
            "export_name" => {
                if let Meta::NameValue(nv) = &a.meta {
                    export_name = expr_lit_str(&nv.value);
                }
                names.push(last);
            }
            "cfg" | "allow" | "warn" | "deny" | "inline" | "must_use" | "derive" => {}
            _ => names.push(last),
        }
    }
    (names, export_name, safety_doc)
}

struct IdentCollector(BTreeSet<String>);
impl<'ast> Visit<'ast> for IdentCollector {
    fn visit_path(&mut self, p: &'ast syn::Path) {
        if p.segments.len() == 1 {
            self.0.insert(p.segments[0].ident.to_string());
        }
        visit::visit_path(self, p);
    }
    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        if let Ok(args) = m.parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated) {
            for a in &args {
                self.visit_expr(a);
            }
        }
    }
}

fn arg_facts(e: &Expr) -> Value {
    let mut c = IdentCollector(BTreeSet::new());
    c.visit_expr(e);
    json!({
        "idents": c.0.into_iter().collect::<Vec<_>>(),
        "closure": matches!(e, Expr::Closure(_)),
        "text": truncate(&quote_tokens(e), 120),
    })
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        s.chars().take(n).collect::<String>() + "…"
    }
}

struct SrcFile {
    rel: String,
    lines: Rc<Vec<String>>,
    ast: syn::File,
    module: Vec<String>,
    dir: PathBuf,
}

#[derive(Default)]
struct Node {
    v: Map<String, Value>,
    calls: Vec<Value>,
    panics: Vec<Value>,
    raises: Vec<Value>,
    unsafe_blocks: Vec<Value>,
}

struct Scanner {
    root: PathBuf,
    conf: Rc<Conf>,
    cfg: Cfg,
    files: Vec<SrcFile>,
    fn_names: HashSet<String>,
    foreign: Vec<Value>,
    foreign_names: HashSet<String>,
    static_muts: HashSet<String>,
    unsafe_impls: Vec<Value>,
    nodes: Vec<Node>,
    errors: Vec<String>,
}

impl Scanner {
    fn load(&mut self, path: &Path, module: Vec<String>, dir: PathBuf) {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) => {
                self.errors.push(format!("{}: {e}", path.display()));
                return;
            }
        };
        let ast = match syn::parse_file(&text) {
            Ok(a) => a,
            Err(e) => {
                self.errors.push(format!("{}: parse: {e}", path.display()));
                return;
            }
        };
        let rel = path.strip_prefix(&self.root).unwrap_or(path).display().to_string();
        let lines = Rc::new(text.lines().map(String::from).collect());
        let items = ast.items.clone();
        self.files.push(SrcFile { rel, lines, ast, module: module.clone(), dir: dir.clone() });
        self.load_mods(&items, &module, &dir, path);
    }

    fn load_mods(&mut self, items: &[Item], module: &[String], dir: &Path, file: &Path) {
        for it in items {
            if let Item::Mod(m) = it {
                if !self.cfg.enabled(&m.attrs) {
                    continue;
                }
                let name = m.ident.to_string();
                let mut child_mod = module.to_vec();
                child_mod.push(name.clone());
                let child_dir = dir.join(&name);
                if let Some((_, inner)) = &m.content {
                    self.load_mods(inner, &child_mod, &child_dir, file);
                    continue;
                }
                let explicit = m.attrs.iter().find(|a| a.path().is_ident("path")).and_then(|a| {
                    if let Meta::NameValue(nv) = &a.meta {
                        expr_lit_str(&nv.value)
                    } else {
                        None
                    }
                });
                let candidates = match explicit {
                    Some(p) => vec![file.parent().unwrap_or(dir).join(p)],
                    None => vec![dir.join(format!("{name}.rs")), dir.join(&name).join("mod.rs")],
                };
                match candidates.iter().find(|p| p.exists()) {
                    Some(p) => {
                        let p = p.clone();
                        self.load(&p, child_mod, child_dir);
                    }
                    None => self.errors.push(format!("module {} not found", child_mod.join("::"))),
                }
            }
        }
    }

    fn collect_names(&mut self, items: &[Item], rel: &str) {
        for it in items {
            match it {
                Item::Fn(f) if self.cfg.enabled(&f.attrs) => {
                    self.fn_names.insert(f.sig.ident.to_string());
                    let nested: Vec<Item> = f
                        .block
                        .stmts
                        .iter()
                        .filter_map(|s| if let syn::Stmt::Item(i) = s { Some(i.clone()) } else { None })
                        .collect();
                    self.collect_names(&nested, rel);
                }
                Item::Impl(i) if self.cfg.enabled(&i.attrs) => {
                    for ii in &i.items {
                        if let syn::ImplItem::Fn(f) = ii {
                            if self.cfg.enabled(&f.attrs) {
                                self.fn_names.insert(f.sig.ident.to_string());
                            }
                        }
                    }
                }
                Item::Trait(t) if self.cfg.enabled(&t.attrs) => {
                    for ti in &t.items {
                        if let syn::TraitItem::Fn(f) = ti {
                            self.fn_names.insert(f.sig.ident.to_string());
                        }
                    }
                }
                Item::Mod(m) if self.cfg.enabled(&m.attrs) => {
                    if let Some((_, inner)) = &m.content {
                        self.collect_names(inner, rel);
                    }
                }
                Item::ForeignMod(fm) if self.cfg.enabled(&fm.attrs) => {
                    let abi = fm.abi.name.as_ref().map(|n| n.value()).unwrap_or_else(|| "C".into());
                    for fi in &fm.items {
                        if let syn::ForeignItem::Fn(f) = fi {
                            if self.cfg.enabled(&f.attrs) {
                                let name = f.sig.ident.to_string();
                                self.foreign_names.insert(name.clone());
                                self.foreign.push(json!({
                                    "name": name, "abi": abi, "file": rel, "line": line_of(&f.sig.ident),
                                }));
                            }
                        }
                    }
                }
                Item::Static(s) if self.cfg.enabled(&s.attrs) => {
                    if matches!(s.mutability, syn::StaticMutability::Mut(_)) {
                        self.static_muts.insert(s.ident.to_string());
                    }
                }
                _ => {}
            }
        }
    }

    fn walk(&mut self, items: &[Item], module: &[String], fi: usize, parent_fn: Option<&str>) {
        for it in items {
            match it {
                Item::Fn(f) => {
                    if !self.cfg.enabled(&f.attrs) {
                        continue;
                    }
                    let name = match parent_fn {
                        Some(p) => format!("{p}::{}", f.sig.ident),
                        None => f.sig.ident.to_string(),
                    };
                    self.emit_fn(fi, module, None, None, &name, &f.attrs, &f.sig, Some(&f.block), "fn");
                }
                Item::Impl(i) => {
                    if !self.cfg.enabled(&i.attrs) {
                        continue;
                    }
                    let self_ty = type_name(&i.self_ty);
                    let trait_ = i.trait_.as_ref().map(|(_, p, _)| path_str(p));
                    if i.unsafety.is_some() {
                        self.unsafe_impls.push(json!({
                            "self_ty": self_ty, "trait": trait_,
                            "file": self.files[fi].rel, "line": line_of(&i.impl_token),
                        }));
                    }
                    for ii in &i.items {
                        if let syn::ImplItem::Fn(f) = ii {
                            if !self.cfg.enabled(&f.attrs) {
                                continue;
                            }
                            let name = format!("{self_ty}::{}", f.sig.ident);
                            self.emit_fn(
                                fi, module, Some(&self_ty), trait_.as_deref(), &name,
                                &f.attrs, &f.sig, Some(&f.block), "method",
                            );
                        }
                    }
                }
                Item::Trait(t) => {
                    if !self.cfg.enabled(&t.attrs) {
                        continue;
                    }
                    let tn = t.ident.to_string();
                    for ti in &t.items {
                        if let syn::TraitItem::Fn(f) = ti {
                            if let Some(b) = &f.default {
                                let name = format!("{tn}::{}", f.sig.ident);
                                self.emit_fn(fi, module, Some(&tn), Some(&tn), &name, &f.attrs, &f.sig, Some(b), "trait-default");
                            }
                        }
                    }
                }
                Item::Mod(m) => {
                    if !self.cfg.enabled(&m.attrs) {
                        continue;
                    }
                    if let Some((_, inner)) = &m.content {
                        let mut child = module.to_vec();
                        child.push(m.ident.to_string());
                        let inner = inner.clone();
                        self.walk(&inner, &child, fi, None);
                    }
                }
                _ => {}
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_fn(
        &mut self,
        fi: usize,
        module: &[String],
        self_ty: Option<&str>,
        trait_: Option<&str>,
        name: &str,
        attrs: &[Attribute],
        sig: &syn::Signature,
        block: Option<&syn::Block>,
        kind: &str,
    ) {
        let (attr_names, export_name, safety_doc) = attr_facts(attrs);
        let id = format!("{}::{name}", module.join("::"));
        let mut params = Vec::new();
        let mut raw_ptrs = HashSet::new();
        for inp in &sig.inputs {
            if let syn::FnArg::Typed(pt) = inp {
                if let syn::Pat::Ident(pi) = &*pt.pat {
                    params.push(pi.ident.to_string());
                    if is_raw_ptr_type(&pt.ty) {
                        raw_ptrs.insert(pi.ident.to_string());
                    }
                } else {
                    params.push("_".into());
                }
            }
        }
        let file = &self.files[fi];
        let mut v = Map::new();
        v.insert("id".into(), json!(id));
        v.insert("name".into(), json!(sig.ident.to_string()));
        v.insert("module".into(), json!(module.join("::")));
        v.insert("self_ty".into(), json!(self_ty));
        v.insert("trait".into(), json!(trait_));
        v.insert("kind".into(), json!(kind));
        v.insert("file".into(), json!(file.rel));
        v.insert("line".into(), json!(line_of(&sig.ident)));
        v.insert("end_line".into(), json!(block.map(end_line_of)));
        v.insert("abi".into(), json!(sig.abi.as_ref().map(|a| a.name.as_ref().map(|n| n.value()).unwrap_or_else(|| "C".into()))));
        v.insert("unsafe_fn".into(), json!(sig.unsafety.is_some()));
        v.insert("safety_doc".into(), json!(safety_doc));
        v.insert("attrs".into(), json!(attr_names));
        v.insert("export_name".into(), json!(export_name));
        v.insert("params".into(), json!(params));
        let idx = self.nodes.len();
        self.nodes.push(Node { v, ..Default::default() });
        let Some(block) = block else { return };
        let lines = file.lines.clone();
        let module_v = module.to_vec();
        let mut bv = BodyV {
            sc: self,
            node: idx,
            lines,
            unsafe_stack: Vec::new(),
            parents: Vec::new(),
            raw_ptrs,
            locals: params.iter().cloned().collect(),
            local_inits: HashMap::new(),
            stmt_line: 0,
            nested: Vec::new(),
        };
        if sig.unsafety.is_some() {
            let bid = bv.open_unsafe(line_of(&sig.ident), end_line_of(block), safety_doc, "unsafe-fn-body");
            bv.unsafe_stack.push(bid);
        }
        bv.visit_block(block);
        let nested = std::mem::take(&mut bv.nested);
        let parent = name.to_string();
        self.walk(&nested, &module_v, fi, Some(&parent));
    }
}

struct BodyV<'s> {
    sc: &'s mut Scanner,
    node: usize,
    lines: Rc<Vec<String>>,
    unsafe_stack: Vec<usize>,
    parents: Vec<(usize, usize)>,
    raw_ptrs: HashSet<String>,
    locals: HashSet<String>,
    local_inits: HashMap<String, String>,
    stmt_line: usize,
    nested: Vec<Item>,
}

impl BodyV<'_> {
    fn n(&mut self) -> &mut Node {
        &mut self.sc.nodes[self.node]
    }

    fn cur_unsafe(&self) -> Option<usize> {
        self.unsafe_stack.last().copied()
    }

    fn parents_json(&self) -> Value {
        json!(self.parents.iter().map(|(c, a)| json!([c, a])).collect::<Vec<_>>())
    }

    fn line_text(&self, line: usize) -> String {
        self.lines.get(line.wrapping_sub(1)).map(|s| s.trim().to_string()).unwrap_or_default()
    }

    /// A `SAFETY` comment on the block's line, its enclosing statement, or the comment run above it.
    fn has_safety(&self, block_line: usize) -> bool {
        let has = |l: usize| self.lines.get(l.wrapping_sub(1)).is_some_and(|s| s.contains("SAFETY"));
        let start = if self.stmt_line > 0 && self.stmt_line <= block_line { self.stmt_line } else { block_line };
        if (start..=block_line + 1).any(has) {
            return true;
        }
        let mut l = start.saturating_sub(1);
        while l > 0 {
            let t = self.line_text(l);
            if t.starts_with("//") || t.starts_with("#[") {
                if t.contains("SAFETY") {
                    return true;
                }
                l -= 1;
            } else {
                break;
            }
        }
        false
    }

    fn open_unsafe(&mut self, line: usize, end: usize, safety: bool, kind: &str) -> usize {
        let id = self.n().unsafe_blocks.len();
        self.n().unsafe_blocks.push(json!({
            "id": id, "line": line, "end_line": end, "safety": safety, "kind": kind, "ops": [],
        }));
        id
    }

    fn op(&mut self, op: &str, detail: &str, line: usize) {
        if let Some(b) = self.cur_unsafe() {
            if let Some(ops) = self.n().unsafe_blocks[b].get_mut("ops").and_then(Value::as_array_mut) {
                ops.push(json!({"op": op, "detail": detail, "line": line}));
            }
        }
    }

    fn push_call(&mut self, rec: Map<String, Value>) -> usize {
        let id = self.n().calls.len();
        let mut rec = rec;
        rec.insert("id".into(), json!(id));
        rec.insert("parents".into(), self.parents_json());
        rec.insert("unsafe".into(), json!(self.cur_unsafe()));
        self.n().calls.push(Value::Object(rec));
        id
    }

    fn visit_args<'a>(&mut self, id: usize, args: impl Iterator<Item = &'a Expr>) {
        for (i, a) in args.enumerate() {
            self.parents.push((id, i));
            self.visit_expr(a);
            self.parents.pop();
        }
    }

    /// A closure handed to a registration call becomes its own node, not part of the caller.
    fn closure_entry(&mut self, c: &syn::ExprClosure, reg_call: &str, reg_args: Vec<String>) {
        let parent = &self.sc.nodes[self.node].v;
        let pid = parent.get("id").and_then(Value::as_str).unwrap_or("").to_string();
        let mut v = Map::new();
        for k in ["module", "file", "self_ty"] {
            v.insert(k.into(), parent.get(k).cloned().unwrap_or(Value::Null));
        }
        let line = line_of(c);
        v.insert("id".into(), json!(format!("{pid}::{{closure@{line}}}")));
        v.insert("name".into(), json!(format!("{{closure@{line}}}")));
        v.insert("kind".into(), json!("closure-entry"));
        v.insert("line".into(), json!(line));
        v.insert("end_line".into(), json!(end_line_of(c)));
        v.insert("parent".into(), json!(pid));
        v.insert("reg_call".into(), json!(reg_call));
        v.insert("reg_args".into(), json!(reg_args));
        v.insert("abi".into(), Value::Null);
        v.insert("unsafe_fn".into(), json!(false));
        v.insert("attrs".into(), json!([]));
        v.insert("params".into(), json!([]));
        let idx = self.sc.nodes.len();
        self.sc.nodes.push(Node { v, ..Default::default() });
        let saved = (
            self.node,
            std::mem::take(&mut self.unsafe_stack),
            std::mem::take(&mut self.parents),
        );
        self.node = idx;
        self.visit_expr(&c.body);
        self.node = saved.0;
        self.unsafe_stack = saved.1;
        self.parents = saved.2;
    }

    /// A divisor that cannot be zero: a literal, a SCREAMING_CASE constant, or a clamped local.
    fn nonzero(&self, e: &Expr) -> bool {
        let e = match e {
            Expr::Paren(p) => &*p.expr,
            Expr::Cast(c) => &*c.expr,
            other => other,
        };
        match e {
            Expr::Lit(_) => true,
            Expr::Path(p) => {
                let last = p.path.segments.last().map(|s| s.ident.to_string()).unwrap_or_default();
                if last.len() > 1 && last.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_') {
                    return true;
                }
                self.local_inits.get(&last).is_some_and(|init| {
                    ["clamp(1", "clamp(2", "max(1", "max(2", "NonZero"].iter().any(|p| init.contains(p))
                })
            }
            Expr::MethodCall(m) => m.method == "get" && quote_tokens(&*m.receiver).contains("NonZero"),
            _ => false,
        }
    }

    fn record_panic(&mut self, kind: &str, line: usize) {
        let rec = json!({"kind": kind, "line": line, "parents": self.parents_json(), "unsafe": self.cur_unsafe()});
        self.n().panics.push(rec);
    }
}

fn stmt_attrs(s: &syn::Stmt) -> &[Attribute] {
    match s {
        syn::Stmt::Local(l) => &l.attrs,
        syn::Stmt::Macro(m) => &m.attrs,
        syn::Stmt::Item(_) => &[],
        syn::Stmt::Expr(e, _) => expr_attrs(e),
    }
}

fn expr_attrs(e: &Expr) -> &[Attribute] {
    match e {
        Expr::Call(x) => &x.attrs,
        Expr::MethodCall(x) => &x.attrs,
        Expr::Block(x) => &x.attrs,
        Expr::Unsafe(x) => &x.attrs,
        Expr::If(x) => &x.attrs,
        Expr::Match(x) => &x.attrs,
        Expr::Assign(x) => &x.attrs,
        Expr::Macro(x) => &x.attrs,
        Expr::Path(x) => &x.attrs,
        Expr::Return(x) => &x.attrs,
        Expr::Let(x) => &x.attrs,
        Expr::ForLoop(x) => &x.attrs,
        Expr::While(x) => &x.attrs,
        Expr::Loop(x) => &x.attrs,
        _ => &[],
    }
}

impl<'ast> Visit<'ast> for BodyV<'_> {
    fn visit_stmt(&mut self, s: &'ast syn::Stmt) {
        if !self.sc.cfg.enabled(stmt_attrs(s)) {
            return;
        }
        if let syn::Stmt::Item(it) = s {
            if self.sc.cfg.enabled(match it {
                Item::Fn(f) => &f.attrs,
                Item::Impl(i) => &i.attrs,
                _ => &[],
            }) {
                self.nested.push(it.clone());
            }
            return;
        }
        let saved = self.stmt_line;
        self.stmt_line = line_of(s);
        if let syn::Stmt::Local(l) = s {
            if let syn::Pat::Type(pt) = &l.pat {
                if let syn::Pat::Ident(pi) = &*pt.pat {
                    if is_raw_ptr_type(&pt.ty) {
                        self.raw_ptrs.insert(pi.ident.to_string());
                    }
                }
            }
            if let (syn::Pat::Ident(pi), Some(init)) = (&l.pat, &l.init) {
                self.local_inits.insert(pi.ident.to_string(), quote_tokens(&*init.expr).replace(' ', ""));
                let raw = match &*init.expr {
                    Expr::Cast(c) => is_raw_ptr_type(&c.ty),
                    Expr::MethodCall(m) => m.method == "as_ptr" || m.method == "as_mut_ptr",
                    _ => false,
                };
                if raw {
                    self.raw_ptrs.insert(pi.ident.to_string());
                }
            }
        }
        visit::visit_stmt(self, s);
        self.stmt_line = saved;
    }

    fn visit_arm(&mut self, a: &'ast syn::Arm) {
        if self.sc.cfg.enabled(&a.attrs) {
            visit::visit_arm(self, a);
        }
    }

    fn visit_pat_ident(&mut self, p: &'ast syn::PatIdent) {
        self.locals.insert(p.ident.to_string());
        visit::visit_pat_ident(self, p);
    }

    fn visit_expr_unsafe(&mut self, u: &'ast syn::ExprUnsafe) {
        let line = line_of(&u.unsafe_token);
        let safety = self.has_safety(line);
        let id = self.open_unsafe(line, end_line_of(u), safety, "block");
        self.unsafe_stack.push(id);
        visit::visit_expr_unsafe(self, u);
        self.unsafe_stack.pop();
    }

    fn visit_expr_call(&mut self, c: &'ast syn::ExprCall) {
        let line = line_of(c);
        let path = if let Expr::Path(p) = &*c.func { Some(path_segs(&p.path)) } else { None };
        if let Some(segs) = &path {
            let last = segs.last().cloned().unwrap_or_default();
            if self.sc.conf.closure_entries.contains(&last) {
                let others: Vec<String> = c
                    .args
                    .iter()
                    .filter(|a| !matches!(a, Expr::Closure(_)))
                    .map(|a| truncate(&quote_tokens(a), 120))
                    .collect();
                let mut rec = Map::new();
                rec.insert("line".into(), json!(line));
                rec.insert("path".into(), json!(segs));
                rec.insert("args".into(), json!([]));
                rec.insert("registers_closure".into(), json!(true));
                self.push_call(rec);
                for a in &c.args {
                    match a {
                        Expr::Closure(cl) => self.closure_entry(cl, &segs.join("::"), others.clone()),
                        Expr::Macro(_) | Expr::Path(_) | Expr::Lit(_) => {}
                        other => self.visit_expr(other),
                    }
                }
                return;
            }
        }
        let mut rec = Map::new();
        rec.insert("line".into(), json!(line));
        rec.insert("args".into(), json!(c.args.iter().map(arg_facts).collect::<Vec<_>>()));
        let strs: Vec<String> = c.args.iter().filter_map(expr_lit_str).collect();
        if !strs.is_empty() {
            rec.insert("str_args".into(), json!(strs));
        }
        match &path {
            Some(segs) => {
                let last = segs.last().cloned().unwrap_or_default();
                let ffi = if segs[..segs.len().saturating_sub(1)].iter().any(|s| self.sc.conf.ffi_modules.contains(s))
                    || (segs.len() == 1 && self.sc.foreign_names.contains(&last))
                {
                    Some(last.clone())
                } else {
                    None
                };
                rec.insert("path".into(), json!(segs));
                if let Some(sym) = &ffi {
                    rec.insert("ffi".into(), json!(sym));
                    rec.insert("via".into(), json!(if segs.len() == 1 { "extern-block" } else { "ffi-module" }));
                    self.op("ffi", sym, line);
                } else if self.sc.conf.unsafe_apis.contains(&last) {
                    self.op("unsafe-api", &segs.join("::"), line);
                } else {
                    self.op("call", &segs.join("::"), line);
                }
            }
            None => {
                rec.insert("path".into(), Value::Null);
                rec.insert("indirect".into(), json!(truncate(&quote_tokens(&*c.func), 80)));
                self.op("indirect-call", &truncate(&quote_tokens(&*c.func), 80), line);
            }
        }
        let id = self.push_call(rec);
        if path.is_none() {
            self.visit_expr(&c.func);
        }
        self.visit_args(id, c.args.iter());
    }

    fn visit_expr_method_call(&mut self, m: &'ast syn::ExprMethodCall) {
        let line = line_of(&m.method);
        let name = m.method.to_string();
        self.visit_expr(&m.receiver);
        if self.sc.conf.panic_methods.contains(&name) {
            self.record_panic(&name, line);
        }
        let recv_self = matches!(&*m.receiver, Expr::Path(p) if p.path.is_ident("self"));
        let mut rec = Map::new();
        rec.insert("line".into(), json!(line));
        rec.insert("method".into(), json!(name));
        rec.insert("recv_self".into(), json!(recv_self));
        rec.insert("recv".into(), json!(truncate(&quote_tokens(&*m.receiver), 80)));
        rec.insert("args".into(), json!(m.args.iter().map(arg_facts).collect::<Vec<_>>()));
        let strs: Vec<String> = m.args.iter().filter_map(expr_lit_str).collect();
        if !strs.is_empty() {
            rec.insert("str_args".into(), json!(strs));
        }
        if self.sc.conf.unsafe_methods.contains(&name) {
            self.op("unsafe-method?", &name, line);
        } else {
            self.op("method", &name, line);
        }
        let id = self.push_call(rec);
        self.visit_args(id, m.args.iter());
    }

    fn visit_expr_path(&mut self, p: &'ast syn::ExprPath) {
        let segs = path_segs(&p.path);
        let last = segs.last().cloned().unwrap_or_default();
        let line = line_of(p);
        if segs.len() == 1 && self.sc.static_muts.contains(&last) {
            self.op("static-mut", &last, line);
        }
        if segs.iter().rev().skip(1).any(|s| self.sc.conf.ffi_modules.contains(s)) {
            // A pg_sys global or constant read; globals are linked data symbols.
            let is_const = last.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
            if !is_const {
                self.op("ffi-global", &last, line);
            }
        }
        let local = segs.len() == 1 && self.locals.contains(&last);
        if self.sc.fn_names.contains(&last) && !local {
            let mut rec = Map::new();
            rec.insert("line".into(), json!(line));
            rec.insert("path".into(), json!(segs));
            rec.insert("ref".into(), json!(true));
            rec.insert("ctx".into(), json!(truncate(&self.line_text(line), 160)));
            rec.insert("args".into(), json!([]));
            self.push_call(rec);
        }
        visit::visit_expr_path(self, p);
    }

    fn visit_expr_unary(&mut self, u: &'ast syn::ExprUnary) {
        if matches!(u.op, syn::UnOp::Deref(_)) && self.cur_unsafe().is_some() {
            let (op, detail) = match &*u.expr {
                Expr::Path(p) if p.path.segments.len() == 1 => {
                    let n = p.path.segments[0].ident.to_string();
                    (if self.raw_ptrs.contains(&n) { "raw-deref" } else { "deref" }, n)
                }
                other => ("deref", truncate(&quote_tokens(other), 60)),
            };
            self.op(op, &detail, line_of(u));
        }
        visit::visit_expr_unary(self, u);
    }

    fn visit_expr_index(&mut self, i: &'ast syn::ExprIndex) {
        self.record_panic("index", line_of(i));
        visit::visit_expr_index(self, i);
    }

    fn visit_expr_binary(&mut self, b: &'ast syn::ExprBinary) {
        if matches!(b.op, syn::BinOp::Div(_) | syn::BinOp::Rem(_)) && !self.nonzero(&b.right) {
            self.record_panic("div", line_of(b));
        }
        visit::visit_expr_binary(self, b);
    }

    fn visit_expr_closure(&mut self, c: &'ast syn::ExprClosure) {
        for inp in &c.inputs {
            if let syn::Pat::Ident(pi) = inp {
                self.locals.insert(pi.ident.to_string());
            }
        }
        visit::visit_expr_closure(self, c);
    }

    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        let name = m.path.segments.last().map(|s| s.ident.to_string()).unwrap_or_default();
        let line = line_of(&m.path);
        if self.sc.conf.panic_macros.contains(&name) {
            self.record_panic(&format!("{name}!"), line);
        }
        if let Some(level) = self.sc.conf.raise_macros.get(&name).cloned() {
            let level = if level == "dynamic" {
                let t = m.tokens.to_string();
                ["PANIC", "FATAL", "ERROR"]
                    .iter()
                    .find(|l| t.split(|c: char| !c.is_alphanumeric() && c != '_').any(|w| w == **l))
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "below-ERROR".into())
            } else {
                level
            };
            let rec = json!({"macro": name, "level": level, "line": line, "parents": self.parents_json(), "unsafe": self.cur_unsafe()});
            self.n().raises.push(rec);
        }
        if let Some(sym) = self.sc.conf.macro_ffi.get(&name).cloned() {
            let mut rec = Map::new();
            rec.insert("line".into(), json!(line));
            rec.insert("path".into(), json!([format!("{name}!")]));
            rec.insert("ffi".into(), json!(sym));
            rec.insert("via".into(), json!("macro"));
            rec.insert("args".into(), json!([]));
            self.op("ffi", &sym, line);
            self.push_call(rec);
        }
        if let Ok(args) = m.parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated) {
            for a in &args {
                self.visit_expr(a);
            }
        } else if let Ok(block) = m.parse_body_with(syn::Block::parse_within) {
            for s in &block {
                self.visit_stmt(s);
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |flag: &str| args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).cloned();
    let Some(src) = get("--src") else {
        eprintln!("usage: ffi-footprint-scanner --src <crate dir> [--features a,b] [--cfg k=v,..] [--config conf.json]");
        std::process::exit(2);
    };
    let features: HashSet<String> =
        get("--features").map(|f| f.split(',').filter(|s| !s.is_empty()).map(String::from).collect()).unwrap_or_default();
    let set: HashSet<String> = get("--cfg")
        .unwrap_or_else(|| "unix,target_os=linux,target_family=unix,target_pointer_width=64,target_endian=little,panic=unwind".into())
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.replace('"', ""))
        .collect();
    let conf_json: Value = match get("--config") {
        Some(p) => match std::fs::read_to_string(&p).map(|t| serde_json::from_str(&t)) {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                eprintln!("{p}: {e}");
                std::process::exit(2);
            }
            Err(e) => {
                eprintln!("{p}: {e}");
                std::process::exit(2);
            }
        },
        None => json!({}),
    };
    let root = PathBuf::from(&src);
    let lib = ["src/lib.rs", "src/main.rs"].iter().map(|p| root.join(p)).find(|p| p.exists());
    let Some(lib) = lib else {
        eprintln!("{src}: no src/lib.rs or src/main.rs");
        std::process::exit(2);
    };
    let mut sc = Scanner {
        root: root.clone(),
        conf: Rc::new(Conf::from_json(&conf_json)),
        cfg: Cfg { features: features.clone(), set, unknown: BTreeSet::new() },
        files: Vec::new(),
        fn_names: HashSet::new(),
        foreign: Vec::new(),
        foreign_names: HashSet::new(),
        static_muts: HashSet::new(),
        unsafe_impls: Vec::new(),
        nodes: Vec::new(),
        errors: Vec::new(),
    };
    let src_dir = lib.parent().map(Path::to_path_buf).unwrap_or_default();
    sc.load(&lib, vec!["crate".into()], src_dir);
    for i in 0..sc.files.len() {
        let items = sc.files[i].ast.items.clone();
        let rel = sc.files[i].rel.clone();
        sc.collect_names(&items, &rel);
    }
    for i in 0..sc.files.len() {
        let items = sc.files[i].ast.items.clone();
        let module = sc.files[i].module.clone();
        let _ = &sc.files[i].dir;
        sc.walk(&items, &module, i, None);
    }
    let nodes: Vec<Value> = sc
        .nodes
        .into_iter()
        .map(|n| {
            let mut v = n.v;
            v.insert("calls".into(), Value::Array(n.calls));
            v.insert("panics".into(), Value::Array(n.panics));
            v.insert("raises".into(), Value::Array(n.raises));
            v.insert("unsafe_blocks".into(), Value::Array(n.unsafe_blocks));
            Value::Object(v)
        })
        .collect();
    let mut features: Vec<String> = features.into_iter().collect();
    features.sort();
    let out = json!({
        "tool": "ffi-footprint-scanner",
        "src": src,
        "features": features,
        "files": sc.files.iter().map(|f| f.rel.clone()).collect::<Vec<_>>(),
        "functions": nodes,
        "foreign_fns": sc.foreign,
        "static_muts": sc.static_muts.into_iter().collect::<BTreeSet<_>>(),
        "unsafe_impls": sc.unsafe_impls,
        "cfg_unknown": sc.cfg.unknown,
        "errors": sc.errors,
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
}

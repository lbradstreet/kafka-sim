//! Reviewed assets and HTML-safe data are separate trust boundaries.
use crate::{
    ExperimentBundle, ExperimentReport, catalogue,
    cli::{Index, io},
    js_wrapper, report,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};
const MAX_HTML_BYTES: usize = 64 * 1024 * 1024;
const MAX_GALLERY_BYTES: usize = 16 * 1024 * 1024;
const ASSETS: [&str; 9] = [
    "trace-viewer-core.js",
    "producer-comparison-model.js",
    "producer-experiment-model.js",
    "trace-viewer-ui.js",
    "producer-experiment-viewer.js",
    "producer-comparison-viewer.js",
    "trace-viewer.css",
    "producer-experiment.css",
    "producer-experiment.html",
];
struct Assets(BTreeMap<String, String>);
impl Assets {
    fn load(root: &Path) -> Result<Self, String> {
        let pins: BTreeMap<String, String> =
            serde_json::from_str(include_str!("export/asset-hashes.json"))
                .map_err(|e| e.to_string())?;
        if pins.len() != ASSETS.len() {
            return Err("asset pin inventory".into());
        }
        let mut sources = BTreeMap::new();
        for name in ASSETS {
            let bytes = io::read(&root.join(name), 1024 * 1024)?;
            let digest = Sha256::digest(&bytes)
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>();
            if pins.get(name) != Some(&digest) {
                return Err(format!(
                    "reviewed asset SHA-256 mismatch: {name}; inspect changes before refreshing pins"
                ));
            }
            let source = String::from_utf8(bytes).map_err(|e| e.to_string())?;
            if (name.ends_with(".js") && source.to_ascii_lowercase().contains("</script"))
                || (name.ends_with(".css") && source.to_ascii_lowercase().contains("</style"))
            {
                return Err(format!("inline source terminator in {name}"));
            }
            sources.insert(name.into(), source);
        }
        Ok(Self(sources))
    }
    fn source(&self, name: &str) -> &str {
        &self.0[name]
    }
}
fn asset_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/trace-tool")
}
fn html_text(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
fn filename(id: &str, page: usize) -> Result<String, String> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b".-".contains(&b))
        || id == "."
        || id == ".."
    {
        return Err("export scenario filename".into());
    }
    Ok(if page == 0 {
        format!("{id}.html")
    } else {
        format!("{id}-page{}.html", page + 1)
    })
}
fn replace_once(source: &mut String, needle: &str, value: &str) -> Result<(), String> {
    if source.matches(needle).count() != 1 {
        return Err(format!("pinned template must contain exactly one {needle}"));
    }
    *source = source.replacen(needle, value, 1);
    Ok(())
}
fn page_html(assets: &Assets, bundle: &ExperimentBundle) -> Result<String, String> {
    bundle.validate()?;
    let id = bundle.scenario["id"].as_str().ok_or("scenario ID")?;
    let page = bundle.page["index"].as_u64().ok_or("page index")? as usize;
    let count = bundle.page["count"].as_u64().ok_or("page count")? as usize;
    // Bound navigation work independently of the artifact's integer fields.
    if count > 4096 {
        return Err("HTML page count bound".into());
    }
    let mut html = assets.source("producer-experiment.html").to_owned();
    replace_once(
        &mut html,
        "<title>Producer experiment explorer</title>",
        &format!(
            "<title>{}</title>",
            html_text(bundle.scenario["title"].as_str().ok_or("title")?)
        ),
    )?;
    for name in ["trace-viewer.css", "producer-experiment.css"] {
        replace_once(
            &mut html,
            &format!("<link rel=\"stylesheet\" href=\"./{name}\">"),
            &format!("<style>\n{}\n</style>", assets.source(name)),
        )?;
    }
    for name in [
        "trace-viewer-core.js",
        "producer-comparison-model.js",
        "producer-experiment-model.js",
        "producer-experiment-data.js",
        "trace-viewer-ui.js",
        "producer-comparison-viewer.js",
        "producer-experiment-viewer.js",
    ] {
        let code = if name == "producer-experiment-data.js" {
            format!(
                "{}\n{}{};",
                js_wrapper::COMMENT,
                js_wrapper::ASSIGNMENT,
                js_wrapper::html_json(bundle)?
            )
        } else {
            assets.source(name).to_owned()
        };
        replace_once(
            &mut html,
            &format!("<script src=\"./{name}\"></script>"),
            &format!("<script>\n{code}\n</script>"),
        )?;
    }
    let mut nav = String::from(
        "<nav aria-label=\"Experiment gallery pages\"><a href=\"index.html\">All experiments</a>",
    );
    if page > 0 {
        nav.push_str(&format!(
            " · <a href=\"{}\">Previous page</a>",
            filename(id, page - 1)?
        ));
    }
    if page + 1 < count {
        nav.push_str(&format!(
            " · <a href=\"{}\">Next page</a>",
            filename(id, page + 1)?
        ));
    }
    nav.push_str(&format!(" · Page {} of {count}</nav>", page + 1));
    replace_once(
        &mut html,
        "<main id=\"experiment\">",
        &format!("<main id=\"experiment\">\n{nav}"),
    )?;
    if html.len() > MAX_HTML_BYTES {
        return Err("HTML page exceeds 64 MiB".into());
    }
    Ok(html)
}
fn write_html(path: &Path, html: &str, cap: usize) -> Result<(), String> {
    io::write_bytes(path, html.as_bytes(), cap)
}
struct GalleryEntry {
    scenario: Value,
    pages: Vec<String>,
    rows: Vec<String>,
}
fn sparkline(report: &ExperimentReport) -> Result<String, String> {
    let values = report.buckets["global"]["acked"]
        .as_array()
        .ok_or("acked buckets")?;
    let max = values
        .iter()
        .filter_map(Value::as_u64)
        .max()
        .unwrap_or(0)
        .max(1) as f64;
    let mut points = String::new();
    for (i, v) in values.iter().enumerate() {
        use std::fmt::Write;
        let x = 2.0 + 236.0 * i as f64 / values.len().saturating_sub(1).max(1) as f64;
        let y = 38.0 - 34.0 * v.as_u64().ok_or("acked count")? as f64 / max;
        write!(&mut points, "{x:.2},{y:.2} ").map_err(|e| e.to_string())?;
    }
    Ok(format!(
        "<svg viewBox=\"0 0 240 42\" width=\"240\" height=\"42\" role=\"img\" aria-label=\"Acknowledged records per time bucket, scaled to this run's peak\"><polyline points=\"{points}\" fill=\"none\" stroke=\"var(--variant-1)\" stroke-width=\"1.5\"/></svg>"
    ))
}
fn gallery_row(r: &ExperimentReport) -> Result<String, String> {
    let s = &r.summary;
    let records = &s["records"];
    Ok(format!(
        "<tr><th scope=\"row\">{} / seed {}</th><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
        html_text(r.meta["variant"]["name"].as_str().ok_or("variant name")?),
        html_text(r.meta["seed"].as_str().ok_or("seed")?),
        records["offered"],
        records["acked"],
        records["refused"],
        html_text(&s["latency_acked"]["p99"].to_string()),
        if r.meta["replay_verified"] == true {
            "verified"
        } else {
            "unverified"
        },
        sparkline(r)?
    ))
}
fn gallery_html(
    assets: &Assets,
    entries: &[GalleryEntry],
    failed: usize,
) -> Result<String, String> {
    let mut html = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>Producer experiment gallery</title><style>{}\n{}</style></head><body><main><header><p class=\"eyebrow\">Replayable producer experiments</p><h1>Experiment gallery</h1><p>Exact whole-population summaries. Sparklines show acknowledged records per bucket; each run has its own vertical scale. Latency is acceptance to consumed acknowledgment.</p><p>{failed} indexed failed runs. Failed runs without validated reports have no chart page; their evidence remains in the source index.</p></header>",
        assets.source("trace-viewer.css"),
        assets.source("producer-experiment.css")
    );
    for category in ["baseline", "hard", "soft", "topology", "resources"] {
        let group: Vec<_> = entries
            .iter()
            .filter(|e| e.scenario["category"] == category)
            .collect();
        if group.is_empty() {
            continue;
        }
        html.push_str(&format!("<section><h2>{}</h2>", html_text(category)));
        for e in group {
            let title = e.scenario["title"].as_str().ok_or("title")?;
            html.push_str(&format!(
                "<article><h3><a href=\"{}\">{}</a></h3><p>{}</p><p>",
                html_text(&e.pages[0]),
                html_text(title),
                html_text(e.scenario["description"].as_str().ok_or("description")?)
            ));
            for (i, p) in e.pages.iter().enumerate() {
                html.push_str(&format!("<a href=\"{}\">Page {}</a> ", html_text(p), i + 1));
            }
            html.push_str("</p><div class=\"table-scroll\"><table><thead><tr><th>Variant / seed</th><th>Offered</th><th>Acked</th><th>Refused</th><th>Ack p99 (ns)</th><th>Replay</th><th>Delivery pattern</th></tr></thead><tbody>");
            for row in &e.rows {
                html.push_str(row);
            }
            html.push_str("</tbody></table></div></article>");
            if html.len() > MAX_GALLERY_BYTES {
                return Err("gallery exceeds 16 MiB".into());
            }
        }
        html.push_str("</section>");
    }
    html.push_str("</main></body></html>");
    Ok(html)
}
/// Load individual validated reports from a bounded CLI index, rebuild bounded
/// pages from those reports, and emit local-only pages plus a gallery.
pub fn export_index(source: &Path, destination: &Path) -> Result<(), String> {
    let index = Index::load(source)?;
    let assets = Assets::load(&asset_root())?;
    let known: BTreeSet<_> = catalogue().iter().map(|s| s.id).collect();
    let ids: BTreeSet<_> = index.runs.iter().map(|r| r.scenario.as_str()).collect();
    let mut entries = vec![];
    for id in ids {
        if !known.contains(id) {
            return Err(format!("unknown export catalogue ID {id}"));
        }
        let mut runs = vec![];
        let mut variants = BTreeMap::new();
        let mut seeds = BTreeSet::new();
        for row in index.runs.iter().filter(|r| r.scenario == id) {
            let Some(path) = &row.report else {
                continue;
            };
            let r = ExperimentReport::from_json(&io::read(
                &io::imported(source, path)?,
                report::MAX_REPORT_BYTES,
            )?)?;
            if r.meta["scenario"]["id"] != id
                || r.meta["variant"]["name"] != row.variant.name
                || r.meta["variant"]["deltas"]
                    != serde_json::to_value(&row.variant.params).map_err(|e| e.to_string())?
                || r.meta["seed"] != row.seed
                || r.meta["size"] != serde_json::to_value(row.size).map_err(|e| e.to_string())?
                || r.meta["replay_verified"] != row.replay_verified
                || r.meta["source"] != row.versions
            {
                return Err("index/report export identity mismatch".into());
            }
            variants.entry(row.variant.name.clone()).or_insert_with(
                || json!({"name":row.variant.name,"deltas":row.variant.params,"order":runs.len()}),
            );
            seeds.insert(row.seed.clone());
            runs.push(r);
        }
        if runs.is_empty() {
            continue;
        }
        let comparisons = index
            .bundles
            .iter()
            .find(|b| b.scenario == id)
            .map(|b| b.comparisons.clone())
            .unwrap_or_default();
        let bundle = ExperimentBundle {
            schema: report::BUNDLE_SCHEMA.into(),
            scenario: runs[0].meta["scenario"].clone(),
            variants: variants.into_values().collect(),
            seeds: seeds.into_iter().collect(),
            page: json!({"index":0,"count":1,"total_runs":runs.len()}),
            comparisons,
            runs,
        };
        let pages = bundle.paginate()?;
        let mut entry = GalleryEntry {
            scenario: bundle.scenario.clone(),
            pages: vec![],
            rows: vec![],
        };
        for (i, page) in pages.iter().enumerate() {
            let name = filename(id, i)?;
            let html = page_html(&assets, page)?;
            write_html(&destination.join(&name), &html, MAX_HTML_BYTES)?;
            entry.pages.push(name);
        }
        for r in &bundle.runs {
            entry.rows.push(gallery_row(r)?);
        }
        entries.push(entry);
    }
    if entries.is_empty() {
        return Err("index contains no validated reports to export".into());
    }
    let gallery = gallery_html(
        &assets,
        &entries,
        index.runs.iter().filter(|r| r.status == "failed").count(),
    )?;
    write_html(&destination.join("index.html"), &gallery, MAX_GALLERY_BYTES)?;
    eprintln!(
        "Exported {} scenario families: {}",
        entries.len(),
        destination.join("index.html").display()
    );
    Ok(())
}
#[cfg(test)]
mod tests;

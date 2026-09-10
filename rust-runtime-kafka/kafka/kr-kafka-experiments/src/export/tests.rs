use super::*;
fn sample() -> ExperimentBundle {
    let text = std::fs::read_to_string(asset_root().join("producer-experiment-data.js")).unwrap();
    let json = text
        .strip_prefix(&format!(
            "{}\n{}",
            js_wrapper::COMMENT,
            js_wrapper::ASSIGNMENT
        ))
        .unwrap()
        .strip_suffix(";\n")
        .unwrap();
    serde_json::from_str(json).unwrap()
}
#[test]
fn html_export_preserves_script_terminators_as_inert_text() {
    let assets = Assets::load(&asset_root()).unwrap();
    let mut b = sample();
    let hostile = "</script><ScRiPt>globalThis.hostile=true</sCrIpT><img src=x onerror='evil()'> &lt; & \" ' \u{2028} \u{2029}";
    b.scenario["title"] = json!(hostile);
    b.scenario["description"] = json!(hostile);
    b.runs[0].meta["scenario"] = b.scenario.clone();
    b.runs[0].environment["bands"][0]["label"] = json!(hostile);
    let html = page_html(&assets, &b).unwrap();
    assert_eq!(html.matches("<script>").count(), 7);
    assert_eq!(html.to_ascii_lowercase().matches("</script>").count(), 7);
    assert!(!html.contains("<img"));
    assert!(!html.contains("<ScRiPt>"));
    assert!(!html.contains("script src="));
    assert!(!html.contains("stylesheet\" href="));
    let embedded = html
        .split(js_wrapper::ASSIGNMENT)
        .nth(1)
        .unwrap()
        .split(";\n</script>")
        .next()
        .unwrap();
    let recovered: ExperimentBundle = serde_json::from_str(embedded).unwrap();
    assert_eq!(recovered, b);
    assert!(!embedded.contains(['<', '>', '&', '\u{2028}', '\u{2029}']));
    let entry = GalleryEntry {
        scenario: b.scenario.clone(),
        pages: vec![filename("sample.broker-crash", 0).unwrap()],
        rows: vec![gallery_row(&b.runs[0]).unwrap()],
    };
    let gallery = gallery_html(&assets, &[entry], 0).unwrap();
    assert!(!gallery.contains("<img"));
    assert!(gallery.contains("&lt;/script&gt;"));
    assert!(gallery.contains("href=\"sample.broker-crash.html\""));
}
#[test]
fn export_rejects_unreviewed_assets_and_unsafe_filenames() {
    let root = std::env::temp_dir().join(format!("experiment-pins-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for file in ASSETS {
        std::fs::copy(asset_root().join(file), root.join(file)).unwrap();
    }
    std::fs::write(root.join(ASSETS[0]), "unreviewed").unwrap();
    assert!(
        Assets::load(&root)
            .err()
            .unwrap()
            .contains("SHA-256 mismatch")
    );
    for id in ["../escape", "/absolute", "..", "UPPER", "a\"onclick", "a/b"] {
        assert!(filename(id, 0).is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn export_paginates_run_and_byte_bounds_with_local_navigation() {
    let assets = Assets::load(&asset_root()).unwrap();
    let mut b = sample();
    let first = b.runs[0].clone();
    b.runs.clear();
    b.seeds.clear();
    for seed in 0..33 {
        let mut r = first.clone();
        r.meta["seed"] = json!(seed.to_string());
        b.runs.push(r);
        b.seeds.push(seed.to_string());
    }
    let pages = b.paginate().unwrap();
    assert_eq!(pages.len(), 2);
    assert_eq!(pages[0].runs.len(), 32);
    assert_eq!(pages[1].runs.len(), 1);
    let one = page_html(&assets, &pages[0]).unwrap();
    let two = page_html(&assets, &pages[1]).unwrap();
    assert!(one.contains("href=\"sample.broker-crash-page2.html\""));
    assert!(two.contains("href=\"sample.broker-crash.html\""));
    assert!(one.contains("Page 1 of 2"));
    assert!(two.contains("Page 2 of 2"));
    // Legitimately bounded optional metadata forces a byte split below 32 runs.
    b.runs.truncate(13);
    b.seeds.truncate(13);
    for r in &mut b.runs {
        r.meta["byte_cap_probe"] = json!(vec!["x".repeat(4000); 1024]);
        r.validate().unwrap();
    }
    let pages = b.paginate().unwrap();
    assert!(pages.len() > 1);
    assert!(pages.iter().all(|p| p.runs.len() < 32));
    for p in pages {
        assert!(serde_json::to_vec(&p).unwrap().len() <= report::MAX_BUNDLE_BYTES);
    }
}

use crate::Hdt;
use spareval::{InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset};
use spargebra::SparqlParser;
use spargebra::term::{BlankNode, NamedNode, Term};
use std::io::{Error, ErrorKind};
use std::str::FromStr;

/// Create the correct term for a given resource string.
/// Slow, use the appropriate method if you know which type (Literal, URI, or blank node) the string has.
// Based on https://github.com/KonradHoeffner/hdt/blob/871db777db3220dc4874af022287975b31d72d3a/src/hdt_graph.rs#L64
fn hdt_bgp_str_to_term(s: &str) -> Result<Term, Error> {
    match s.chars().next() {
        None => Err(Error::new(ErrorKind::InvalidData, "empty input")),
        // Double-quote delimiters are used around the string.
        Some('"') => match Term::from_str(s) {
            Ok(s) => Ok(s),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, format!("literal parse error {e} for {s}"))),
        },
        // Underscore prefix indicating a Blank Node.
        Some('_') => match BlankNode::from_str(s) {
            Ok(n) => Ok(n.into()),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, format!("blanknode parse error {e} for {s}"))),
        },
        // Double-quote delimiters not present. Underscore prefix
        // not present. Assuming a URI.
        _ => {
            // Note that Term::from_str() will not work for URIs (NamedNode) when the string is not within "<" and ">" delimiters.
            match NamedNode::new(s) {
                Ok(n) => Ok(n.into()),
                Err(e) => Err(Error::new(ErrorKind::InvalidData, format!("iri parse error {e} for {s}"))),
            }
        }
    }
}

/// Convert triple string formats from OxRDF to HDT.
fn term_to_hdt_bgp_str(term: Term) -> String {
    match term {
        Term::NamedNode(named_node) => named_node.into_string(),
        Term::Literal(literal) => literal.to_string(),
        Term::BlankNode(s) => s.to_string(),
    }
}

impl<'a> QueryableDataset<'a> for &'a Hdt {
    type InternalTerm = String;
    type Error = Error;

    fn internal_quads_for_pattern(
        &self, subject: Option<&String>, predicate: Option<&String>, object: Option<&String>,
        graph_name: Option<Option<&String>>,
    ) -> impl Iterator<Item = Result<InternalQuad<Self::InternalTerm>, Error>> + use<'a> {
        if let Some(Some(graph_name)) = graph_name {
            return vec![Err(Error::new(
                ErrorKind::InvalidData,
                format!("HDT does not support named graph: {graph_name:?}"),
            ))]
            .into_iter();
        }
        let [ps, pp, po] = [subject, predicate, object].map(|x| x.map(String::as_str));
        // Query HDT for BGP by string values.
        let v: Vec<_> = self
            .triples_with_pattern(ps, pp, po)
            .map(|at| at.map(|a| a.to_string()))
            .map(|[subject, predicate, object]| Ok(InternalQuad { subject, predicate, object, graph_name: None }))
            .collect();
        v.into_iter()
    }

    fn internalize_term(&self, term: Term) -> Result<String, Error> {
        Ok(term_to_hdt_bgp_str(term))
    }

    fn externalize_term(&self, term: String) -> Result<Term, Error> {
        hdt_bgp_str_to_term(&term)
    }
}

pub fn query<'a>(q: &str, hdt: &'a Hdt) -> Result<spareval::QueryResults<'a>, QueryEvaluationError> {
    let query = SparqlParser::new().parse_query(q)?;
    //.unwrap_or_else(|_| panic!("error processing SPARQL query:\n{q}"));
    QueryEvaluator::new().execute(hdt, &query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use color_eyre::Result;
    use fs_err::{File, OpenOptions, create_dir_all};
    use sophia::api::graph::Graph;
    use sophia::api::ns::{Namespace, rdf};
    use sophia::api::parser::TripleParser;
    use sophia::api::prelude::{Any, Triple, TripleSerializer, TripleSource};
    use sophia::api::term::{SimpleTerm, Term};
    use sophia::inmem::graph::FastGraph;
    use sophia::turtle::serializer::nt::NtSerializer;
    use std::collections::HashMap;
    use std::io::{BufReader, Write};
    use std::path::{Path, PathBuf};

    #[test]
    fn select() -> Result<()> {
        init();
        let filename = "tests/resources/snikmeta.hdt";
        let file = File::open(filename)?;
        let hdt = Hdt::read(BufReader::new(file))?;
        let t = [
            "<http://www.snik.eu/ontology/meta/хобби-N-0>", "<http://www.w3.org/2000/01/rdf-schema#label>",
            "\"ХОББИ\"@ru", "\"Anwenden einer Methode123\"", "\"Anwenden einer Methode\"@de",
        ];
        let [s, p, o, _, _] = t;
        let base = "BASE <http://example.org/>";
        let queries = [
            format!("SELECT ?x {{?x  {p} {o}}}"),
            format!("SELECT ?x {{{s} ?x  {o}}}"),
            format!("SELECT ?x {{{s} {p} ?x }}"),
            format!("SELECT (CONCAT(?y,'123') AS ?x) {{?s {p} ?y }} ORDER BY ?x LIMIT 1"),
            format!("{base} SELECT ?x {{ {{?s {p} ?x }} UNION {{<a> <b> ?x}} }} ORDER BY ?x LIMIT 1"),
        ];
        for i in 0..queries.len() {
            let res = query(&queries[i], &hdt)?;

            match res {
                spareval::QueryResults::Solutions(solutions) => {
                    let mut solutions: Vec<_> = solutions.collect();
                    assert_eq!(1, solutions.len());
                    let solution = solutions.pop().unwrap()?;
                    //assert_eq!(solution.get("o"), Some(&Literal::new_language_tagged_literal("ХОББИ", "ru")?.into()));
                    assert_eq!(t[i], solution.get("x").unwrap().to_string());
                }
                _ => {
                    panic!("SELECT query results expected but got something else")
                }
            }
        }
        Ok(())
    }

    const MF: Namespace<&str> =
        Namespace::new_unchecked_const("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#");
    const QT: Namespace<&str> =
        Namespace::new_unchecked_const("http://www.w3.org/2001/sw/DataAccess/tests/test-query#");

    #[derive(Debug)]
    struct TestCase {
        data: PathBuf,
        query: PathBuf,
        _result: PathBuf,
    }

    fn find_ttl_files<P: AsRef<std::path::Path>>(dir: P) -> Vec<PathBuf> {
        walkdir::WalkDir::new(dir)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .map(walkdir::DirEntry::into_path)
            .filter(|e| e.extension().is_some_and(|ext| ext == "ttl"))
            .collect()
    }

    fn convert_to_nt(source_rdf: &Path, dest_nt: &Path) -> Result<()> {
        let rdf_file = File::open(source_rdf)?;
        let reader = BufReader::new(rdf_file);
        let nt_file = File::options().read(true).write(true).create(true).truncate(true).open(dest_nt)?;
        let mut writer = std::io::BufWriter::new(nt_file);
        let mut graph = sophia::inmem::graph::LightGraph::default();
        let mut sophia_serializer = NtSerializer::new(writer.by_ref());

        if source_rdf.extension().is_some_and(|ext| ext == ".ttl") {
            let ttl_parser = sophia::turtle::parser::turtle::TurtleParser {
                base: Some(sophia::iri::Iri::new(format!(
                    "file://{}",
                    std::path::Path::new(source_rdf).file_name().unwrap().to_str().unwrap()
                ))?),
            };

            ttl_parser.parse(reader).add_to_graph(&mut graph)?;
        }
        // TODO .rdf file format?

        sophia_serializer.serialize_graph(&graph)?;
        writer.flush()?;
        Ok(())
    }

    fn load_manifest(path: &Path) -> Result<FastGraph, Box<dyn std::error::Error>> {
        let file = BufReader::new(File::open(path)?);
        let mut graph = sophia::inmem::graph::FastGraph::default();
        let ttl_parser = sophia::turtle::parser::turtle::TurtleParser {
            base: Some(sophia::iri::Iri::new(format!(
                "file://{}/#",
                std::path::Path::new(path).parent().unwrap().to_str().unwrap()
            ))?),
        };

        ttl_parser.parse(file).add_to_graph(&mut graph)?;
        Ok(graph)
    }

    fn parse_manifest(path: &Path) -> Result<Vec<TestCase>, Box<dyn std::error::Error>> {
        let g = load_manifest(path)?;

        let mut cases = Vec::new();

        // Find the manifest node
        let p = MF.get("entries")?;
        let manifest_nodes: Vec<_> = g.triples_matching(Any, [&p], Any).collect();
        if manifest_nodes.is_empty() {
            return Ok(cases);
        }

        // The object of mf:entries is the head of an RDF list
        let list_head = manifest_nodes[0]?.o().clone();

        // Walk the RDF list
        let mut current = list_head;
        while !current.is_iri() || current.iri().unwrap() != rdf::nil.to_iriref() {
            if let Some(test) = g
                .triples_matching([&current], [&rdf::first, &MF.get("QueryEvaluationTest")?], Any)
                .next()
                .map(|t| t.unwrap().o().clone())
            {
                // find mf:action
                if let Some(action_node) =
                    g.triples_matching([&test], [&MF.get("action")?], Any).next().map(|t| t.unwrap().o().clone())
                {
                    // find qt:data
                    let data = g.triples_matching([&action_node], [&QT.get("data")?], Any).next().map(|t| match t
                        .unwrap()
                        .o()
                    {
                        SimpleTerm::BlankNode(b) => b.to_string(),
                        SimpleTerm::Iri(i) => i.to_string(),
                        SimpleTerm::LiteralDatatype(a, _b) => a.to_string(),
                        SimpleTerm::LiteralLanguage(a, _b) => a.to_string(),
                        SimpleTerm::Triple(_t) => todo!(),
                        SimpleTerm::Variable(v) => v.to_string(),
                    });

                    // find qt:query
                    let query =
                        g.triples_matching([&action_node], [&QT.get("query")?], Any).next().map(|t| {
                            match t.unwrap().o() {
                                SimpleTerm::BlankNode(b) => b.to_string(),
                                SimpleTerm::Iri(i) => i.to_string(),
                                SimpleTerm::LiteralDatatype(a, _b) => a.to_string(),
                                SimpleTerm::LiteralLanguage(a, _b) => a.to_string(),
                                SimpleTerm::Triple(_t) => todo!(),
                                SimpleTerm::Variable(v) => v.to_string(),
                            }
                        });
                    // find mf:result
                    let result = g.triples_matching([&test], [&MF.get("result")?], Any).next().map(|t| {
                        match t.unwrap().o() {
                            SimpleTerm::BlankNode(b) => b.to_string(),
                            SimpleTerm::Iri(i) => i.to_string(),
                            SimpleTerm::LiteralDatatype(a, _b) => a.to_string(),
                            SimpleTerm::LiteralLanguage(a, _b) => a.to_string(),
                            SimpleTerm::Triple(_t) => todo!(),
                            SimpleTerm::Variable(v) => v.to_string(),
                        }
                    });

                    if let (Some(data), Some(query), Some(result)) = (data, query, result) {
                        cases.push(TestCase {
                            data: PathBuf::from(data.replace("file://", "")),
                            query: PathBuf::from(query.replace("file://", "")),
                            _result: PathBuf::from(result.replace("file://", "")),
                        });
                    }
                }
            }

            let s = current.clone();
            if let Some(next) = g.triples_matching([&s], [&rdf::rest], Any).next().map(|t| t.unwrap().o().clone())
            {
                current = next;
            } else {
                break;
            }
        }

        Ok(cases)
    }

    // fn parse_srx(path: &str, actual: spareval::QueryResults) -> Result<(), Box<dyn std::error::Error>> {
    //     let file = File::open(path)?;
    //     let reader = BufReader::new(file);
    //     let parser = sparesults::QueryResultsParser::from_format(sparesults::QueryResultsFormat::Xml);
    //     let res = parser.for_reader(reader)?;

    //     match (res, actual) {
    //         (spareval::QueryResults::Solutions(mut exp), spareval::QueryResults::Solutions(mut act)) => {
    //             // Canonicalize order, because SPARQL results are a multiset
    //             exp.sort();
    //             act.sort();

    //             if exp == act {
    //                 println!("✅ Results match!");
    //             } else {
    //                 println!("❌ Results differ:\nExpected: {:?}\nActual: {:?}", exp, act);
    //             }
    //         }
    //         (spareval::QueryResults::Boolean(exp), spareval::QueryResults::Boolean(act)) => {
    //             assert_eq!(exp, act, "Boolean results differ!");
    //         }
    //         (spareval::QueryResults::Graph(exp), spareval::QueryResults::Graph(act)) => {
    //             panic!("hdt does not support named graphs")
    //         }
    //         _ => {
    //             panic!("Result kinds differ between actual and expected!");
    //         }
    //     }
    //     Ok(())
    // }

    #[test]
    #[cfg(feature = "sophia")]
    fn w3c_tests() -> Result<()> {
        use std::io::{BufWriter, Write};
        let mut count = 0;
        let mut skipped = 0;
        for sparql_test_version in ["sparql10", "sparql11", "sparql12"] {
            let input_files = find_ttl_files(format!("tests/resources/rdf-tests/sparql/{sparql_test_version}"));
            assert!(!input_files.is_empty(), "no SPARQL test input found, is rdf-tests submodule checked out?");
            let mut cases = HashMap::new();
            for p in &input_files {
                let parent_folder_name = p.parent().unwrap().file_name().unwrap().to_str().unwrap();
                if p.ends_with("/manifest.ttl") && parent_folder_name != sparql_test_version {
                    cases.insert(parent_folder_name, parse_manifest(p).expect("msg"));
                    continue;
                }
                if p.ends_with("/manifest.ttl") || parent_folder_name == sparql_test_version {
                    continue;
                }

                let nt_file_name = format!(
                    "tests/resources/generated/nt/{sparql_test_version}/{}/{}.nt",
                    parent_folder_name,
                    p.file_stem().unwrap().to_str().unwrap()
                );
                let nt_file_path = Path::new(&nt_file_name);
                create_dir_all(format!(
                    "tests/resources/generated/nt/{sparql_test_version}/{parent_folder_name}"
                ))?;
                convert_to_nt(p, nt_file_path)?;
                let h = Hdt::read_nt(nt_file_path)?;

                let hdt_file_path = format!(
                    "tests/resources/generated/hdt/{sparql_test_version}/{}/{}.hdt",
                    parent_folder_name,
                    p.file_stem().unwrap().to_str().unwrap()
                );
                create_dir_all(Path::new(&hdt_file_path).parent().unwrap())?;
                let out_file = OpenOptions::new().create(true).write(true).truncate(true).open(&hdt_file_path)?;
                let mut writer = BufWriter::new(out_file);
                h.write(&mut writer)?;
                writer.flush()?;
                assert!(Path::new(&hdt_file_path).exists());
            }
            for (folder, test_cases) in cases {
                let mut folder_count = 0;
                for case in &test_cases {
                    use crate::sparql;
                    use color_eyre::eyre::WrapErr;
                    use std::io::Read;

                    // currently only converting TTL -> NT -> HDT
                    if case.data.extension().unwrap() != "ttl" {
                        continue;
                    }

                    // changed recently // empty datasets are ignored, an HDT file with no triples is invalid
                    /*if case.data.ends_with("empty.ttl") {
                        continue;
                    }*/

                    // println!("{folder}:  {:?}", case);
                    let hdt_name = format!(
                        "tests/resources/generated/hdt/{sparql_test_version}/{}/{}.hdt",
                        folder,
                        case.data.file_stem().unwrap().to_str().unwrap()
                    );

                    let hdt = Hdt::read(BufReader::new(File::open(hdt_name)?))?;
                    let mut query_str = String::new();
                    BufReader::new(File::open(&case.query)?).read_to_string(&mut query_str)?;
                    // we don't support graphs
                    if query_str.contains("graph <") {
                        continue;
                    }
                    folder_count += 1;
                    let _res = sparql::query(&query_str, &hdt).wrap_err_with(|| {
                        log::error!("{}", case.query.to_str().unwrap());
                        format!("Error with SPARQL query:\n{query_str}\nfor case {:?}", case.data)
                    })?;
                    log::info!("{} ... ok", case.query.to_str().unwrap());
                }
                if folder_count > 0 {
                    log::info!("{folder_count} w3c tests ok in folder {sparql_test_version}/{folder}");
                }
                let folder_skipped = test_cases.len() - folder_count;
                if folder_skipped > 0 {
                    log::warn!(
                        "{folder_skipped}/{} w3c tests skipped in folder {sparql_test_version}/{folder}",
                        test_cases.len()
                    );
                }
                count += folder_count;
                skipped += folder_skipped;
            }
        }
        log::info!("{count} total w3c tests ok, {skipped} tests skipped");
        #[cfg(feature = "sparql")]
        fs_err::remove_dir_all("tests/resources/generated")?;
        Ok(())
    }
}

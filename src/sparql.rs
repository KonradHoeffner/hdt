use crate::{Hdt, hdt};
use spareval::{InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset};
use spargebra::Query;
use spargebra::term::{BlankNode, NamedNode, Term};
use std::io::{Error, ErrorKind};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone)]
/// Boundry over a Header-Dictionary-Triplies (HDT) storage layer.
pub struct HdtDataset {
    hdts: Arc<Vec<Arc<Hdt>>>,
}

impl HdtDataset {
    pub fn new(paths: &[&str]) -> Result<Self, hdt::Error> {
        let mut hdts: Vec<Arc<Hdt>> = Vec::new();
        for path in paths {
            // TODO catch error and proceed to next file?
            #[cfg(feature = "cache")]
            let hdt = Hdt::new_from_path(std::path::Path::new(&path))?;
            #[cfg(not(feature = "cache"))]
            let hdt = {
                let file = std::fs::File::open(path)?;
                Hdt::read(std::io::BufReader::new(file))?
            };
            hdts.push(Arc::new(hdt));
        }
        Ok(Self { hdts: Arc::new(hdts) })
    }
}

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

impl QueryableDataset for HdtDataset {
    type InternalTerm = String;
    type Error = Error;

    fn internal_quads_for_pattern(
        &self, subject: Option<&String>, predicate: Option<&String>, object: Option<&String>,
        graph_name: Option<Option<&String>>,
    ) -> Box<dyn Iterator<Item = Result<InternalQuad<Self>, Error>>> {
        if let Some(Some(graph_name)) = graph_name {
            return Box::new(std::iter::once(Err(Error::new(
                ErrorKind::InvalidData,
                format!("HDT does not support named graph: {graph_name:?}"),
            ))));
        }
        let mut v: Vec<Result<InternalQuad<_>, Error>> = Vec::new();
        for hdt in self.hdts.iter() {
            // Query HDT for BGP by string values.
            let [ps, pp, po] = [subject, predicate, object].map(|x| x.map(String::as_str));
            let results = hdt.triples_with_pattern(ps, pp, po);
            for r in results {
                let [subject, predicate, object] = r.map(|a| a.to_string());
                v.push(Ok(InternalQuad { subject, predicate, object, graph_name: None }));
            }
        }
        Box::new(v.into_iter())
    }

    fn internalize_term(&self, term: Term) -> Result<String, Error> {
        Ok(term_to_hdt_bgp_str(term))
    }

    fn externalize_term(&self, term: String) -> Result<Term, Error> {
        hdt_bgp_str_to_term(&term)
    }
}

pub fn query(q: &str, ds: HdtDataset) -> Result<spareval::QueryResults, QueryEvaluationError> {
    let query = Query::parse(q, None).unwrap_or_else(|_| panic!("error processing query {q}"));
    QueryEvaluator::new().execute(ds, &query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use color_eyre::Result;
    //use fs_err::File;

    #[test]
    fn select() -> Result<()> {
        init();
        let filename = "tests/resources/snikmeta.hdt";
        //let file = File::open(filename)?;
        //let hdt = Hdt::read(std::io::BufReader::new(file))?;
        let ds = HdtDataset::new(&[filename])?;
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
            let res = query(&queries[i], ds.clone())?;

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
}

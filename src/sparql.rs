use crate::{Hdt, hdt};
use oxrdf::{NamedNode, Term};
use spareval::{InternalQuad, QueryEvaluationError, QueryEvaluator, QueryableDataset};
use spargebra::Query;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone)]
pub struct HDTDataset {
    /// HDT interface.
    hdt: Arc<Hdt>,
}

#[derive(Clone)]
/// Boundry over a Header-Dictionary-Triplies (HDT) storage layer.
pub struct HDTDatasetView {
    // collection of HDT files in the dataset
    hdts: Vec<HDTDataset>,
}

impl HDTDatasetView {
    pub fn new(paths: &[String]) -> Result<Self, hdt::Error> {
        let mut hdts: Vec<HDTDataset> = Vec::new();
        for path in paths {
            // TODO catch error and proceed to next file?
            #[cfg(feature = "cache")]
            let hdt = Hdt::new_from_path(std::path::Path::new(&path))?;

            #[cfg(not(feature = "cache"))]
            let hdt = {
                let file = std::fs::File::open(path)?;
                Hdt::read(std::io::BufReader::new(file))?
            };

            hdts.push(HDTDataset { hdt: Arc::new(hdt) });
        }

        Ok(Self { hdts })
    }
}

/// Create the correct OxRDF term for a given resource string.  Slow,
/// use the appropriate method if you know which type (Literal, URI,
/// or blank node) the string has. Based on
/// https://github.com/KonradHoeffner/hdt/blob/871db777db3220dc4874af022287975b31d72d3a/src/hdt_graph.rs#L64
fn hdt_bgp_str_to_term(s: &str) -> Result<Term, Error> {
    match s.chars().next() {
        None => Err(Error::new(ErrorKind::InvalidData, "empty input")),

        // Double-quote delimters are used around the string.
        Some('"') => match Term::from_str(s) {
            Ok(s) => Ok(s),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, format!("literal parse error {e} for {s}"))),
        },

        // Underscore prefix indicating a Blank Node.
        Some('_') => match oxrdf::BlankNode::from_str(s) {
            Ok(n) => Ok(n.into()),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, format!("blanknode parse error {e} for {s}"))),
        },

        // Double-quote delimiters not present. Underscore prefix
        // not present. Assuming a URI.
        _ => {
            // Note that Term::from_str() will not work for URIs
            // (OxRDF NamedNode) when the string is not within "<"
            // and ">" delimiters.
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

impl QueryableDataset for HDTDatasetView {
    type InternalTerm = String;
    type Error = Error;

    fn internal_quads_for_pattern(
        &self, subject: Option<&String>, predicate: Option<&String>, object: Option<&String>,
        graph_name: Option<Option<&String>>,
    ) -> Box<dyn Iterator<Item = Result<InternalQuad<Self>, Error>>> {
        if let Some(graph_name) = graph_name {
            if graph_name.is_some() {
                return Box::new(
                    vec![Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("HDT does not support named graph: {graph_name:?}"),
                    ))]
                    .into_iter(),
                );
            }
        }

        // Create a vector to hold the results.
        let mut v: Vec<Result<InternalQuad<_>, Error>> = Vec::new();

        for data in &self.hdts {
            // Query HDT for BGP by string values.
            let results = data.hdt.triples_with_pattern(
                subject.map(|s| s.as_str()),
                predicate.map(|s| s.as_str()),
                object.map(|s| s.as_str()),
            );

            // For each result
            for result in results {
                let ex_s = (*result.0).to_string();
                let ex_p = (*result.1).to_string();
                let ex_o = (*result.2).to_string();

                // Add the result to the vector.
                v.push(Ok(InternalQuad { subject: ex_s, predicate: ex_p, object: ex_o, graph_name: None }));
            }
        }

        Box::new(v.into_iter())
    }

    fn internalize_term(&self, term: Term) -> Result<String, Error> {
        Ok(term_to_hdt_bgp_str(term))
    }

    fn externalize_term(&self, term: String) -> Result<Term, Error> {
        match hdt_bgp_str_to_term(&term) {
            Ok(s) => Ok(s),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, format!("Unexpected externalize bug {e}"))),
        }
    }
}

pub fn evaluate_hdt_query(
    q: &str,
    dataset: HDTDatasetView,
    //rq: &Path, dataset: HDTDatasetView,
) -> Result<
    (Result<spareval::QueryResults, QueryEvaluationError>, spareval::QueryExplanation),
    Box<dyn std::error::Error>,
> {
    //let q = std::fs::read_to_string(rq).expect("error reading sparql query file");
    let query = Query::parse(q, None).expect(&format!("error processing query {q}"));
    //let query = Query::parse(q.as_str(), None).expect("error processing query");
    Ok(QueryEvaluator::new().explain(dataset, &query))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use color_eyre::Result;
    use oxrdf::Literal;

    #[test]
    fn select() -> Result<()> {
        init();
        let filename = "tests/resources/snikmeta.hdt";
        let t = [
            "<http://www.snik.eu/ontology/meta/хобби-N-0>", "<http://www.w3.org/2000/01/rdf-schema#label>",
            "\"ХОББИ\"@ru", "\"Anwenden einer Methode123\"", "\"Anwenden einer Methode123\"",
        ];
        let [s, p, o, _, _] = t;
        let queries = [
            format!("SELECT ?x {{?x  {p} {o}}}"),
            format!("SELECT ?x {{{s} ?x  {o}}}"),
            format!("SELECT ?x {{{s} {p} ?x }}"),
            format!("SELECT (CONCAT(?y,'123') AS ?x) {{?s {p} ?y }} ORDER BY ?x LIMIT 1"),
            format!(
                "SELECT (CONCAT(?y,'123') AS ?x) {{ {{?s {p} ?y }} UNION {{<a> <b> ?y}} }} ORDER BY ?x LIMIT 1"
            ),
        ];
        for i in 0..5 {
            let dataset = HDTDatasetView::new(&[filename.to_string()])?;
            let (res, _explaination) =
            //evaluate_hdt_query(std::path::Path::new(queryfile), dataset).expect("failed to evaluate SPARQL query");
            evaluate_hdt_query(&queries[i], dataset).expect("failed to evaluate SPARQL query");

            let res = res.expect("error with SPARQL query results");
            if let spareval::QueryResults::Solutions(solutions) = res {
                let mut solutions: Vec<_> = solutions.collect();
                assert_eq!(1, solutions.len());
                let solution = solutions.pop().unwrap()?;
                //assert_eq!(solution.get("o"), Some(&Literal::new_language_tagged_literal("ХОББИ", "ru")?.into()));
                assert_eq!(t[i], solution.get("x").unwrap().to_string());
            } else {
                panic!("")
            }
        }
        Ok(())
    }
}

//! *This module is available only if HDT is built with the `"sophia"` feature.*
#[cfg(feature = "sophia")]
use crate::four_sect_dict::IdKind;
use crate::hdt::Hdt;
use sophia::graph::*;
use sophia::term::iri::Iri;
use sophia::term::literal::Literal;
use sophia::term::*;
use sophia::triple::stream::*;
use sophia::triple::streaming_mode::*;
use std::convert::Infallible;

/// Adapter to use HDT as a Sophia graph.
pub struct HdtGraph {
    hdt: Hdt,
}

impl HdtGraph {
    /// Wrapper around Hdt.
    pub const fn new(hdt: Hdt) -> Self {
        HdtGraph { hdt }
    }
    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.hdt.size_in_bytes()
    }
}

fn auto_term(s: String) -> Result<BoxTerm, TermError> {
    match s.chars().next() {
        None => Err(TermError::InvalidIri(String::new())),
        Some('"') => match s.rfind('"') {
            None => Err(TermError::UnsupportedDatatype(s)),
            Some(index) => {
                let lex = s[1..index].to_owned();
                let rest = &s[index + 1..];
                // literal with no language tag and no datatype
                if rest.is_empty() {
                    return Ok(BoxTerm::from(lex));
                }
                // either language tag or datatype
                if let Some(tag_index) = rest.find('@') {
                    return Ok(BoxTerm::from(Literal::new_lang_unchecked(lex, rest[tag_index + 1..].to_owned())));
                }
                // datatype
                let mut dt_split = rest.split("^^");
                dt_split.next(); // empty
                match dt_split.next() {
                    Some(dt) => Ok(BoxTerm::from(Literal::new_dt(lex, Iri::<&str>::new(&dt[1..dt.len() - 1])?))),
                    None => Err(TermError::UnsupportedDatatype(s)),
                }
            }
        },
        Some('_') => BoxTerm::new_bnode(s[2..].to_owned()),
        _ => BoxTerm::new_iri(s),
    }
}

// transforms string triples into a sophia TripleSource
fn triple_source<'s>(triples: impl Iterator<Item = (String, String, String)> + 's) -> GTripleSource<'s, HdtGraph> {
    Box::new(
        triples
            .map(|(s, p, o)| -> Result<_> {
                debug_assert_ne!("", s, "triple_source subject is empty   ({s}, {p}, {o})");
                debug_assert_ne!("", p, "triple_source predicate is empty ({s}, {p}, {o})");
                debug_assert_ne!("", o, "triple_source object is empty    ({s}, {p}, {o})");
                Ok(StreamedTriple::by_value([auto_term(s)?, auto_term(p)?, auto_term(o)?]))
            })
            .filter_map(|r| r.map_err(|e| eprintln!("{e}")).ok())
            .into_triple_source(),
    )
}

// Sophia doesn't include the _: prefix for blank node strings but HDT expects it
// not needed for property terms, as they can't be blank nodes
fn term_string(t: &(impl TTerm + ?Sized)) -> String {
    match t.kind() {
        TermKind::BlankNode => "_:".to_owned() + &t.value(),
        TermKind::Iri => t.value().to_string(),
        TermKind::Literal => {
            let value = format!("\"{}\"", t.value());
            if let Some(lang) = t.language() {
                return format!("{value}@{lang}");
            }
            if let Some(dt) = t.datatype() {
                // handle implicit xsd:string datatype
                // TODO check if this breaks triples that have explicit xsd:string datattype
                if dt.value() != "http://www.w3.org/2001/XMLSchema#string" {
                    return format!("{value}^^{dt}");
                }
            }
            value
        }
        TermKind::Variable => {
            panic!("Variable term strings are not supported.");
        }
    }
}

impl Graph for HdtGraph {
    type Triple = ByValue<[BoxTerm; 3]>;
    type Error = Infallible; // infallible for now, figure out what to put here later

    fn triples(&self) -> GTripleSource<Self> {
        eprintln!(
            "Warning: Iterating through ALL triples in the HDT Graph. This can be inefficient for large graphs."
        );
        triple_source(self.hdt.triples())
    }

    fn triples_with_s<'s, TS: TTerm + ?Sized>(&'s self, s: &'s TS) -> GTripleSource<'s, Self> {
        triple_source(self.hdt.triples_with(&term_string(s), &IdKind::Subject))
    }

    fn triples_with_p<'s, TS: TTerm + ?Sized>(&'s self, p: &'s TS) -> GTripleSource<'s, Self> {
        triple_source(self.hdt.triples_with(&p.value(), &IdKind::Predicate))
    }

    fn triples_with_o<'s, TS: TTerm + ?Sized>(&'s self, o: &'s TS) -> GTripleSource<'s, Self> {
        triple_source(self.hdt.triples_with(&term_string(o), &IdKind::Object))
    }

    /// An iterator visiting all triples with the given subject and predicate.
    fn triples_with_sp<'s, TS: TTerm + ?Sized, TP: TTerm + ?Sized>(
        &'s self, s: &'s TS, p: &'s TP,
    ) -> GTripleSource<'s, Self> {
        triple_source(self.hdt.triples_with_sp(&term_string(s), &term_string(p)))
    }

    /// An iterator visiting all triples with the given subject and object.
    fn triples_with_so<'s, TS: TTerm + ?Sized, TO: TTerm + ?Sized>(
        &'s self, s: &'s TS, o: &'s TO,
    ) -> GTripleSource<'s, Self> {
        triple_source(self.hdt.triples_with_so(&term_string(s), &term_string(o)))
    }

    /// An iterator visiting all triples with the given predicate and object.
    fn triples_with_po<'s, TP: TTerm + ?Sized, TO: TTerm + ?Sized>(
        &'s self, p: &'s TP, o: &'s TO,
    ) -> GTripleSource<'s, Self> {
        triple_source(self.hdt.triples_with_po(&term_string(p), &term_string(o)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sophia::triple::Triple;
    use std::fs::File;

    #[test]
    fn test_graph() {
        let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
        let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
        let graph = HdtGraph::new(hdt);
        let triples: Vec<_> = graph.triples().collect();
        assert_eq!(triples.len(), 327);
        assert!(graph
            .triples_with_s(&BoxTerm::new_iri_unchecked("http://www.snik.eu/ontology/meta".to_owned()))
            .next()
            .is_some());
        for uri in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let term = BoxTerm::new_iri_unchecked(uri.to_owned());
            let filtered: Vec<_> = triples
                .iter()
                .map(|triple| triple.as_ref().unwrap())
                .filter(|triple| triple.s().value() == uri)
                .collect();
            let with_s: Vec<_> = graph.triples_with_s(&term).map(std::result::Result::unwrap).collect();
            // Sophia strings can't be compared directly, use the Debug trait for string comparison that is more brittle and less elegant
            // could break in the future e.g. because of ordering
            let filtered_string = format!("{filtered:?}");
            let with_s_string = format!("{with_s:?}");
            assert_eq!(
                filtered_string, with_s_string,
                "different results between triples() and triples_with_s() for {}",
                uri
            );
        }
        let s = BoxTerm::new_iri_unchecked("http://www.snik.eu/ontology/meta/Top".to_owned());
        let p = BoxTerm::new_iri_unchecked("http://www.w3.org/2000/01/rdf-schema#label".to_owned());
        let o = BoxTerm::from(Literal::new_lang_unchecked("top class", "en"));
        assert!(graph.triples_with_o(&o).next().is_some());
        let triple = (&s, &p, &o);

        let sp = graph.triples_with_sp(&s, &p).map(std::result::Result::unwrap).collect::<Vec<_>>();
        let so = graph.triples_with_so(&s, &o).map(std::result::Result::unwrap).collect::<Vec<_>>();
        let po = graph.triples_with_po(&p, &o).map(std::result::Result::unwrap).collect::<Vec<_>>();
        // can't use assert_eq! directly on streaming triple mode to compare triple with result of triples_with_...
        // let triple_vec = vec![(&s,&p,&o)];
        // assert_eq!(triple_vec,graph.triples_with_sp(&s,&p).map(std::result::Result::unwrap).collect::<Vec<_>>());
        // assert_eq!(triple_vec,graph.triples_with_so(&s,&o).map(std::result::Result::unwrap).collect::<Vec<_>>());
        // assert_eq!(triple_vec,graph.triples_with_po(&p,&o).map(std::result::Result::unwrap).collect::<Vec<_>>());
        for vec in [sp, so, po] {
            assert_eq!(1, vec.len());
            let e = &vec[0];
            assert_eq!(e.s(), triple.0);
            assert_eq!(e.p(), triple.1);
            assert_eq!(e.o(), triple.2);
        }
        assert!(graph.triples_with_o(&BoxTerm::from("22.10".to_owned())).count() == 1);
        let date = &BoxTerm::from(Literal::new_dt(
            "2022-10-20",
            Iri::<&str>::new_unchecked("http://www.w3.org/2001/XMLSchema#date"),
        ));
        assert!(graph.triples_with_o(date).count() == 1);
        // not in snik meta but only in local test file to make sure explicit xsd:string works
        /*
        let testo = &BoxTerm::from(Literal::new_dt(
            "testo",
            Iri::<&str>::new_unchecked("http://www.w3.org/2001/XMLSchema#string"),
        ));
        assert!(graph.triples_with_o(testo).count() == 1);
        */
    }
}

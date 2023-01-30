//! *This module is available only if HDT is built with the `"sophia"` feature.*
#[cfg(feature = "sophia")]
use crate::four_sect_dict::IdKind;
use crate::hdt::Hdt;
use crate::triples::{Id, ObjectIter, PredicateIter, PredicateObjectIter, SubjectIter, TripleId};
use log::debug;
use log::error;
use sophia::api::graph::{GTripleSource, Graph};
//use sophia::api::term;
use mownstr::MownStr;
use sophia::api::prelude::Triple;
use sophia::api::source::{IntoTripleSource, TripleSource};
use sophia::api::term::FromTerm;
use sophia::api::term::{matcher::TermMatcher, BnodeId, IriRef, LanguageTag, SimpleTerm, Term};
use std::convert::Infallible;
use std::io::{self, Error, ErrorKind};
use std::iter;

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

    fn term(&self, id: Id, kind: &'static IdKind) -> SimpleTerm<'static> {
        IriRef::new_unchecked(MownStr::from(self.hdt.dict.id_to_string(id, kind).unwrap())).into_term()
    }
}

/// Create the correct Sophia term for a given resource string.
/// Slow, use the appropriate method if you know which type (Literal, URI, or blank node) the string has.
fn auto_term(s: MownStr) -> io::Result<SimpleTerm> {
    match s.chars().next() {
        None => Err(Error::new(ErrorKind::InvalidData, "empty input")),
        Some('"') => match s.rfind('"') {
            None => Err(Error::new(
                ErrorKind::InvalidData,
                format!("missing right quotation mark in literal string {}", s),
            )),
            Some(index) => {
                let lex = &s[1..index];
                let rest = &s[index + 1..];
                // literal with no language tag and no datatype
                if rest.is_empty() {
                    return Ok(lex.into_term());
                }
                let lex = MownStr::from_str(lex);
                // either language tag or datatype
                if let Some(tag_index) = rest.find('@') {
                    let tag = LanguageTag::new_unchecked(MownStr::from_str(&rest[tag_index + 1..]));
                    return Ok(SimpleTerm::from_term(SimpleTerm::LiteralLanguage(lex, tag)));
                }
                // datatype
                let mut dt_split = rest.split("^^");
                dt_split.next(); // empty
                match dt_split.next() {
                    Some(dt) => {
                        let unquoted = &dt[1..dt.len() - 1];
                        let dt = IriRef::new_unchecked(MownStr::from_str(unquoted));
                        Ok(SimpleTerm::from_term(SimpleTerm::LiteralDatatype(lex, dt)))
                    }
                    None => Err(Error::new(ErrorKind::InvalidData, format!("empty datatype in {s}"))),
                }
            }
        },
        Some('_') => Ok(BnodeId::new_unchecked(MownStr::from_str(&s[2..].to_owned())).into_term()),
        _ => Ok(SimpleTerm::Iri(IriRef::new_unchecked(s))),
    }
}

// Convert a SimpleTerm into the HDT String format.
// Sophia doesn't include the _: prefix for blank node strings but HDT expects it
// not needed for property terms, as they can't be blank nodes
fn term_string(t: &SimpleTerm) -> String {
    match t {
        SimpleTerm::BlankNode(b) => "_:".to_owned() + &b.as_str(),
        SimpleTerm::Iri(i) => i.as_str().to_owned(),
        SimpleTerm::LiteralLanguage(l, lang) => {
            format!("{l}@{}", lang.as_str())
        }
        SimpleTerm::LiteralDatatype(l, dt) => {
            format!("{l}@{}", dt.as_str())
        }
        _ => {
            panic!("Variable term strings and RDF-star are not supported.");
        }
    }
}

impl Graph for HdtGraph {
    type Triple<'a> = [SimpleTerm<'a>; 3];
    type Error = Infallible; // infallible for now, figure out what to put here later

    fn triples(&self) -> GTripleSource<Self> {
        debug!("Iterating through ALL triples in the HDT Graph. This can be inefficient for large graphs.");
        Box::new(self.hdt.triples().map(move |(s, p, o)| {
            Ok([auto_term(s).unwrap(), SimpleTerm::Iri(IriRef::new_unchecked(p.into())), auto_term(o).unwrap()])
        }))
    }

    /// Only supports constant and "any" matchers.
    /// Non-constant matchers are supposed to be "any" matchers.
    fn triples_matching<'s, S, P, O>(&'s self, sm: S, pm: P, om: O) -> GTripleSource<'s, Self>
    where
        S: TermMatcher + 's,
        P: TermMatcher + 's,
        O: TermMatcher + 's,
    {
        let xso = sm.constant().map(|s| {
            let simple = SimpleTerm::from_term(s.as_simple());
            let id = self.hdt.dict.string_to_id(&term_string(&simple), &IdKind::Subject);
            (simple, id)
        });
        let xpo = pm.constant().map(|p| {
            let simple = SimpleTerm::from_term(p.as_simple());
            let id = self.hdt.dict.string_to_id(&term_string(&simple), &IdKind::Predicate);
            (simple, id)
        });
        let xoo = om.constant().map(|o| {
            let simple = SimpleTerm::from_term(o.as_simple());
            let id = self.hdt.dict.string_to_id(&term_string(&simple), &IdKind::Object);
            (simple, id)
        });
        if [&xso, &xpo, &xoo].into_iter().flatten().any(|x| x.1 == 0) {
            // at least one term does not exist in the graph
            return Box::new(iter::empty());
        }
        //return Box::new(iter::empty());
        if [&xso, &xpo, &xoo].into_iter().flatten().any(|x| x.1 == 0) {
            // at least one term does not exist in the graph
            return Box::new(iter::empty());
        }
        // TODO: improve error handling
        match (xso, xpo, xoo) {
            (Some(s), Some(p), Some(o)) => {
                if SubjectIter::with_pattern(&self.hdt.triples, &TripleId::new(s.1, p.1, o.1)).next().is_some() {
                    Box::new(iter::once(Ok([s.0, p.0, o.0])))
                } else {
                    Box::new(iter::empty())
                }
            }
            (Some(s), Some(p), None) => {
                Box::new(SubjectIter::with_pattern(&self.hdt.triples, &TripleId::new(s.1, p.1, 0)).map(move |t| {
                    Ok([
                        s.0.clone(),
                        p.0.clone(),
                        IriRef::new_unchecked(MownStr::from(
                            self.hdt.dict.id_to_string(t.object_id, &IdKind::Object).unwrap(),
                        ))
                        .into_term(),
                    ])
                }))
            }
            (Some(s), None, Some(o)) => {
                Box::new(SubjectIter::with_pattern(&self.hdt.triples, &TripleId::new(s.1, 0, o.1)).map(move |t| {
                    Ok([
                        s.0.clone(),
                        IriRef::new_unchecked(MownStr::from(
                            self.hdt.dict.id_to_string(t.predicate_id, &IdKind::Predicate).unwrap(),
                        ))
                        .into_term(),
                        o.0.clone(),
                    ])
                }))
            }
            (Some(s), None, None) => {
                Box::new(SubjectIter::with_pattern(&self.hdt.triples, &TripleId::new(s.1, 0, 0)).map(move |t| {
                    Ok([
                        s.0.clone(),
                        IriRef::new_unchecked(MownStr::from(
                            self.hdt.dict.id_to_string(t.predicate_id, &IdKind::Predicate).unwrap(),
                        ))
                        .into_term(),
                        IriRef::new_unchecked(MownStr::from(
                            self.hdt.dict.id_to_string(t.object_id, &IdKind::Object).unwrap(),
                        ))
                        .into_term(),
                    ])
                }))
            }
            (None, Some(p), Some(o)) => {
                Box::new(PredicateObjectIter::new(&self.hdt.triples, p.1, o.1).map(move |sid| {
                    Ok([
                        IriRef::new_unchecked(MownStr::from(
                            self.hdt.dict.id_to_string(sid, &IdKind::Subject).unwrap(),
                        ))
                        .into_term(),
                        p.0.clone(),
                        o.0.clone(),
                    ])
                }))
            }
            (None, Some(p), None) => Box::new(PredicateIter::new(&self.hdt.triples, p.1).map(move |t| {
                Ok([
                    IriRef::new_unchecked(MownStr::from(
                        self.hdt.dict.id_to_string(t.subject_id, &IdKind::Subject).unwrap(),
                    ))
                    .into_term(),
                    p.0.clone(),
                    IriRef::new_unchecked(MownStr::from(
                        self.hdt.dict.id_to_string(t.object_id, &IdKind::Object).unwrap(),
                    ))
                    .into_term(),
                ])
            })),
            (None, None, Some(o)) => Box::new(ObjectIter::new(&self.hdt.triples, o.1).map(move |t| {
                Ok([
                    IriRef::new_unchecked(MownStr::from(
                        self.hdt.dict.id_to_string(t.subject_id, &IdKind::Subject).unwrap(),
                    ))
                    .into_term(),
                    IriRef::new_unchecked(MownStr::from(
                        self.hdt.dict.id_to_string(t.predicate_id, &IdKind::Predicate).unwrap(),
                    ))
                    .into_term(),
                    o.0.clone(),
                ])
            })),
            (None, None, None) => self.triples(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use sophia::api::term::matcher::Any;
    use std::fs::File;
    use std::rc::Rc;

    #[test]
    fn test_graph() {
        type T = Rc<str>;
        init();
        let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
        let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
        let graph = HdtGraph::new(hdt);
        let triples: Vec<_> = graph.triples().collect();
        assert_eq!(triples.len(), 327);
        let meta_top = "http://www.snik.eu/ontology/meta/Top";
        assert!(graph
            .triples_matching(
                Some(SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from_str(
                    "http://www.snik.eu/ontology/meta"
                )))),
                Any,
                Any
            )
            .next()
            .is_some());
        for uri in [meta_top, "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let term = SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from_str(uri)));
            let filtered: Vec<_> = triples
                .iter()
                .map(|triple| triple.as_ref().unwrap())
                .filter(|triple| triple.s().iri().unwrap().to_string() == uri)
                .collect();
            let with_s: Vec<_> =
                graph.triples_matching(Some(term), Any, Any).map(std::result::Result::unwrap).collect();
            // Sophia strings can't be compared directly, use the Debug trait for string comparison that is more brittle and less elegant
            // could break in the future e.g. because of ordering
            let filtered_string = format!("{filtered:?}");
            let with_s_string = format!("{with_s:?}");
            assert_eq!(
                filtered_string, with_s_string,
                "different results between triples() and triples_with_s() for {uri}"
            );
        }
        /*
        let s = SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from_str(meta_top)));
        let p = SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from_str(
            "http://www.w3.org/2000/01/rdf-schema#label",
        )));
        let o = SimpleTerm::from(Literal::new_lang_unchecked("top class", "en"));
        assert!(graph.triples_matching(&o).next().is_some());
        let triple = (&s, &p, &o);

        let sp = graph.triples_matching(Some(s), Some(p),None).map(std::result::Result::unwrap).collect::<Vec<_>>();
        let so = graph.triples_matching(Some(s), Some(o)).map(std::result::Result::unwrap).collect::<Vec<_>>();
        let po = graph.triples_matching(Some(p), Some(o)).map(std::result::Result::unwrap).collect::<Vec<_>>();
        // can't use assert_eq! directly on streaming triple mode to compare triple with result of triples_with_...
        // let triple_vec = vec![(&s,&p,&o)];
        // assert_eq!(triple_vec,graph.triples_matching(&s,&p).map(std::result::Result::unwrap).collect::<Vec<_>>());
        // assert_eq!(triple_vec,graph.triples_matching(&s,&o).map(std::result::Result::unwrap).collect::<Vec<_>>());
        // assert_eq!(triple_vec,graph.triples_matching(&p,&o).map(std::result::Result::unwrap).collect::<Vec<_>>());
        for vec in [sp, so, po] {
            assert_eq!(1, vec.len());
            let e = &vec[0];
            assert_eq!(e.s(), triple.0);
            assert_eq!(e.p(), triple.1);
            assert_eq!(e.o(), triple.2);
        }
        assert!(graph.triples_matching(&SimpleTerm::from("22.10".to_owned())).count() == 1);
        let date = &SimpleTerm::from(LiteralDatatype(
            "2022-10-20",
            Iri::<&str>::new_unchecked("http://www.w3.org/2001/XMLSchema#date"),
        ));
        assert!(graph.triples_matching(date).count() == 1);
        */
        // not in snik meta but only in local test file to make sure explicit xsd:string works
        /*
        let testo = &SimpleTerm::from(LiteralDatatype(
            "testo",
            Iri::<&str>::new_unchecked("http://www.w3.org/2001/XMLSchema#string"),
        ));
        assert!(graph.triples_matching(testo).count() == 1);
        */
    }
}

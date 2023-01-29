//! *This module is available only if HDT is built with the `"sophia"` feature.*
#[cfg(feature = "sophia")]
use crate::four_sect_dict::IdKind;
use crate::hdt::Hdt;
use crate::triples::{ObjectIter, PredicateIter, PredicateObjectIter, SubjectIter, TripleId};
use log::debug;
use log::error;
use sophia::api::graph::{GTripleSource, Graph};
//use sophia::api::term;
use mownstr::MownStr;
use sophia::api::prelude::Triple;
use sophia::api::source::{IntoTripleSource, TripleSource};
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
}

/// Create the correct Sophia term for a given resource string.
/// Slow, use the appropriate method if you know which type (Literal, URI, or blank node) the string has.
fn auto_term<'a>(s: &'a str) -> io::Result<SimpleTerm<'a>> {
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
                    return Ok(SimpleTerm::LiteralLanguage(lex, tag));
                }
                // datatype
                let mut dt_split = rest.split("^^");
                dt_split.next(); // empty
                match dt_split.next() {
                    Some(dt) => {
                        let unquoted = &dt[1..dt.len() - 1];
                        let dt = IriRef::new_unchecked(MownStr::from_str(unquoted));
                        Ok(SimpleTerm::LiteralDatatype(lex, dt))
                    }
                    None => Err(Error::new(ErrorKind::InvalidData, format!("empty datatype in {s}"))),
                }
            }
        },
        Some('_') => Ok(BnodeId::new_unchecked(MownStr::from_str(&s[2..].to_owned())).into_term()),
        _ => Ok(SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from_str(s)))),
    }
}
/*
// transforms Hdt triples into a sophia TripleSource
fn triple_source<'a>(
    triples: impl Iterator<Item = (MownStr<'a>, MownStr<'a>, MownStr<'a>)> + 'a,
) -> GTripleSource<'a, HdtGraph> {
    Box::new(
        triples
            .map(move |(s, p, o)| {
                /*debug_assert_ne!("", s.as_ref(), "triple_source subject is empty   ({s}, {p}, {o})");
                debug_assert_ne!("", p.as_ref(), "triple_source predicate is empty ({s}, {p}, {o})");
                debug_assert_ne!("", o.as_ref(), "triple_source object is empty    ({s}, {p}, {o})");*/
                //Ok([auto_term(s.as_ref())?, auto_term(p.as_ref())?, auto_term(o.as_ref())?])
                //Ok([MownStr::from(""),MownStr::from(""),MownStr::from("")])
                [MownStr::from(""),MownStr::from(""),MownStr::from("")]
            })
           // .filter_map(|r| r.map_err(|e: Error| error!("{e}")).ok())
    )
}
*/
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
            Ok([
                SimpleTerm::Iri(IriRef::new_unchecked(s.into())),
                SimpleTerm::Iri(IriRef::new_unchecked(p.into())),
                SimpleTerm::Iri(IriRef::new_unchecked("".into())),
            ])
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
            //let simple = s.as_simple();
            //let simple = SimpleTerm::from_term_ref(s);
            let simple = SimpleTerm::from(s.as_simple().to_owned());
            let id = self.hdt.dict.string_to_id(&term_string(&simple), &IdKind::Subject);
            (simple, id)
        });
        let xpo = pm.constant().map(|p| {
            let simple = p.as_simple();
            let id = self.hdt.dict.string_to_id(&term_string(&simple), &IdKind::Predicate);
            (simple, id)
        });
        let xoo = om.constant().map(|o| {
            let simple = o.as_simple();
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
            /*
            (Some(s), Some(p), Some(o)) => {
                if SubjectIter::with_pattern(&self.hdt.triples, &TripleId::new(s.1, p.1, o.1)).next().is_some() {
                    Box::new(iter::once(Ok([s.0, p.0, o.0])))
                } else {
                    Box::new(iter::empty())
                }
            }
            (Some(s), Some(p), None) => {
                Box::new(SubjectIter::with_pattern(&self.hdt.triples, &TripleId::new(s.1, p.1, 0)).map(move |t| {
                    (
                        s.0.clone(),
                        p.0.clone(),
                        MownStr::from(self.hdt.dict.id_to_string(t.object_id, &IdKind::Object).unwrap()),
                    )
                }))
            }
            (Some(s), None, Some(o)) => {
                Box::new(SubjectIter::with_pattern(&self.hdt.triples, &TripleId::new(s.1, 0, o.1)).map(move |t| {
                    (
                        s.0.clone(),
                        MownStr::from(self.hdt.dict.id_to_string(t.predicate_id, &IdKind::Predicate).unwrap()),
                        o.0.clone(),
                    )
                }))
            }
            */
            (Some(s), None, None) => {
                Box::new(SubjectIter::with_pattern(&self.hdt.triples, &TripleId::new(s.1, 0, 0)).map(move |t| {
                    Ok([
                        s.0,
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
            /*
            (None, Some(p), Some(o)) => {
                Box::new(PredicateObjectIter::new(&self.hdt.triples, p.1, o.1).map(move |sid| {
                    (
                        MownStr::from(self.hdt.dict.id_to_string(sid, &IdKind::Subject).unwrap()),
                        p.0.clone(),
                        o.0.clone(),
                    )
                }))
            }
            (None, Some(p), None) => Box::new(PredicateIter::new(&self.hdt.triples, p.1).map(move |t| {
                (
                    MownStr::from(self.hdt.dict.id_to_string(t.subject_id, &IdKind::Subject).unwrap()),
                    p.0.clone(),
                    MownStr::from(self.hdt.dict.id_to_string(t.object_id, &IdKind::Object).unwrap()),
                )
            })),
            (None, None, Some(o)) => Box::new(ObjectIter::new(&self.hdt.triples, o.1).map(move |t| {
                (
                    MownStr::from(self.hdt.dict.id_to_string(t.subject_id, &IdKind::Subject).unwrap()),
                    MownStr::from(self.hdt.dict.id_to_string(t.predicate_id, &IdKind::Predicate).unwrap()),
                    o.0.clone(),
                )
            })),
            (None, None, None) => Box::new(self.hdt.triples()),
            */
            _ => Box::new(iter::empty()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use pretty_assertions::{assert_eq, assert_ne};
    use std::fs::File;

    #[test]
    fn triples() {
        init();
        let filename = "tests/resources/snikmeta.hdt";
        let file = File::open(filename).expect("error opening file");
        let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
        let triples = hdt.triples();
        let v: Vec<StringTriple> = triples.collect();
        assert_eq!(v.len(), 327);
        assert_eq!(v, hdt.triples_with_pattern(None, None, None).collect::<Vec<_>>(), "all triples not equal ???");
        assert_ne!(0, hdt.dict.string_to_id("http://www.snik.eu/ontology/meta", &IdKind::Subject));
        for uri in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let filtered: Vec<_> = v.clone().into_iter().filter(|triple| triple.0.as_ref() == uri).collect();
            let with_s: Vec<_> = hdt.triples_with_pattern(Some(uri), None, None).collect();
            assert_eq!(filtered, with_s, "different results between triples() and triples_with_s() for {}", uri);
        }
        let s = "http://www.snik.eu/ontology/meta/Top";
        let p = "http://www.w3.org/2000/01/rdf-schema#label";
        let o = "\"top class\"@en";
        let triple_vec = vec![(MownStr::from(s), MownStr::from(p), MownStr::from(o))];
        // triple patterns with 2-3 terms
        assert_eq!(triple_vec, hdt.triples_with_pattern(Some(s), Some(p), Some(o)).collect::<Vec<_>>(), "SPO");
    }
}

impl HdtGraph {
    /*
    fn triples_with_s<'s>(&'s self, s: &SimpleTerm) -> GTripleSource<'s, Self> {
        let ss = term_string(s);
        let sid = self.hdt.dict.string_to_id(&ss, &IdKind::Subject);
        if sid == 0 {
            return Box::new(std::iter::empty());
        }
        // very inefficient conversion between TS and Term, TODO improve
        //let s = auto_term(&ss).unwrap();
        Box::new(
            BitmapIter::with_pattern(&self.hdt.triples, &TripleId::new(sid, 0, 0))
                .map(move |tid| -> io::Result<_> {
                    Ok([
                        s.clone(),
                        SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from(
                            self.hdt
                                .dict
                                .id_to_string(tid.predicate_id, &IdKind::Predicate)
                                .unwrap()
                                .into_boxed_str(),
                        ))),
                        auto_term(&self.hdt.dict.id_to_string(tid.object_id, &IdKind::Object).unwrap())?,
                    ])
                })
                .filter_map(|r| r.map_err(|e| error!("{e}")).ok())
                .into_triple_source(),
        )
    }
    */
    /*
        fn triples_with_p<'s>(&'s self, p: &SimpleTerm) -> GTripleSource<'s, Self> {
            triple_source(self.hdt.triples_with(&p.value(), &IdKind::Predicate))
        }

        fn triples_with_o<'s>(&'s self, o: &SimpleTerm) -> GTripleSource<'s, Self> {
            triple_source(self.hdt.triples_with(&term_string(o), &IdKind::Object))
        }
    */
    /*
    /// An iterator visiting all triples with the given subject and predicate.
    fn triples_with_sp<'s>(&'s self, s: &SimpleTerm, p: &SimpleTerm) -> GTripleSource<'s, Self> {
        let ss = term_string(s);
        let ps = term_string(p);
        let sid = self.hdt.dict.string_to_id(&ss, &IdKind::Subject);
        let pid = self.hdt.dict.string_to_id(&ps, &IdKind::Predicate);
        if sid == 0 || pid == 0 {
            return Box::new(std::iter::empty());
        }
        // TODO inefficient conversion, can we somehow use the given s and p directly?
        let s = auto_term(&ss).unwrap();
        let p = auto_term(&ps).unwrap();
        Box::new(
            BitmapIter::with_pattern(&self.hdt.triples, &TripleId::new(sid, pid, 0))
                .map(move |tid| -> io::Result<_> {
                    Ok([
                        s.clone(),
                        p.clone(),
                        auto_term(&self.hdt.dict.id_to_string(tid.object_id, &IdKind::Object).unwrap())?,
                    ])
                })
                .filter_map(|r| r.map_err(|e| error!("{e}")).ok())
                .into_triple_source(),
        )
    }
    */
    /*
        /// An iterator visiting all triples with the given subject and object.
        fn triples_with_so<'s>(&'s self, s: &SimpleTerm, o: &SimpleTerm) -> GTripleSource<'s, Self> {
            triple_source(self.hdt.triples_with_so(&term_string(s), &term_string(o)))
        }

    /// An iterator visiting all triples with the given predicate and object.
    fn triples_with_po<'s>(&'s self, p: &'s SimpleTerm, o: &'s SimpleTerm) -> GTripleSource<'s, Self> {
        let ps = term_string(p);
        let os = term_string(o);
        let pid = self.hdt.dict.string_to_id(&ps, &IdKind::Predicate);
        let oid = self.hdt.dict.string_to_id(&os, &IdKind::Object);
        // predicate can be neither a literal nor a blank node
        //let p = SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from_str(ps)));
        //let o = auto_term(&os).unwrap();
        Box::new(
            PredicateObjectIter::new(&self.hdt.triples, pid, oid)
                .map(move |sid| {
                    [
                        // subject is never a literal, so we can save a lot of CPU time here by not using auto_term
                        // TODO: could subject be a blank node?
                        SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from(
                            self.hdt.dict.id_to_string(sid, &IdKind::Subject).unwrap().into_boxed_str(),
                        ))),
                        p.clone(),
                        o.clone(),
                    ]
                })
                //.filter_map(|r| r.map_err(|e| error!("{e}")).ok())
                .into_triple_source(),
        )
    }
    */
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use std::fs::File;
    use std::rc::Rc;

    #[test]
    fn test_graph() {
        type T = Rc<str>;
        init();
        let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
        let hdt = Hdt::<Rc<str>>::new(std::io::BufReader::new(file)).unwrap();
        let graph = HdtGraph::new(hdt);
        let triples: Vec<_> = graph.triples().collect();
        assert_eq!(triples.len(), 327);
        assert!(graph
            .triples_with_s(&SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from_str(
                "http://www.snik.eu/ontology/meta"
            ))))
            .next()
            .is_some());
        for uri in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let term = SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from_str(uri)));
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
                "different results between triples() and triples_with_s() for {uri}"
            );
        }
        let s = SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from_str("http://www.snik.eu/ontology/meta/Top")));
        let p = SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from_str(
            "http://www.w3.org/2000/01/rdf-schema#label",
        )));
        let o = Term::from(Literal::new_lang_unchecked("top class", "en"));
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
        assert!(graph.triples_with_o(&Term::from("22.10".to_owned())).count() == 1);
        let date = &Term::from(LiteralDatatype(
            "2022-10-20",
            Iri::<&str>::new_unchecked("http://www.w3.org/2001/XMLSchema#date"),
        ));
        assert!(graph.triples_with_o(date).count() == 1);
        // not in snik meta but only in local test file to make sure explicit xsd:string works
        /*
        let testo = &Term::from(LiteralDatatype(
            "testo",
            Iri::<&str>::new_unchecked("http://www.w3.org/2001/XMLSchema#string"),
        ));
        assert!(graph.triples_with_o(testo).count() == 1);
        */
    }
}

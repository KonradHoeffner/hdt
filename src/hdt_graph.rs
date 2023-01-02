//! *This module is available only if HDT is built with the `"sophia"` feature.*
#[cfg(feature = "sophia")]
use crate::four_sect_dict::IdKind;
use crate::hdt::Hdt;
use crate::predicate_object_iter::PredicateObjectIter;
use crate::triples::{BitmapIter, TripleId};
use log::debug;
use log::error;
use sophia::graph::*;
use sophia::term::iri::Iri;
use sophia::term::literal::Literal;
use sophia::term::*;
use sophia::triple::stream::*;
use sophia::triple::streaming_mode::*;
use std::convert::From;
use std::convert::Infallible;
use std::hash::Hash;
use std::marker::PhantomData;

/// Type of String pointer to return
pub trait Data:
    AsRef<str> + Clone + Eq + From<std::boxed::Box<str>> + From<String> + From<&'static str> + Hash
{
}
impl<T> Data for T where
    T: AsRef<str> + Clone + Eq + From<std::boxed::Box<str>> + From<String> + From<&'static str> + Hash
{
}

/// Adapter to use HDT as a Sophia graph.
pub struct HdtGraph<T: Data> {
    hdt: Hdt,
    phantom: PhantomData<T>,
}

impl<T: Data> HdtGraph<T> {
    /// Wrapper around Hdt.
    pub const fn new(hdt: Hdt) -> Self {
        HdtGraph { hdt, phantom: PhantomData {} }
    }
    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.hdt.size_in_bytes()
    }
}

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

/// Create the correct Sophia term for a given resource string.
/// Slow, use the appropriate method if you know which type (Literal, URI, or blank node) the string has.
fn auto_term<T: Data>(s: &str) -> Result<Term<T>, TermError> {
    match s.chars().next() {
        None => Err(TermError::InvalidIri(String::new())),
        Some('"') => match s.rfind('"') {
            None => Err(TermError::UnsupportedDatatype(s.to_owned())),
            Some(index) => {
                let lex = s[1..index].to_owned();
                let rest = &s[index + 1..];
                // literal with no language tag and no datatype
                if rest.is_empty() {
                    //let dt_string: sophia::term::iri::Iri<&str> = Iri::<&str>::new_unchecked(XSD_STRING);
                    let dt_string = Iri::<&str>::new_unchecked(XSD_STRING);
                    return Ok(Term::<T>::from(Literal::new_dt(lex, dt_string)));
                }
                // either language tag or datatype
                if let Some(tag_index) = rest.find('@') {
                    return Ok(Term::<T>::from(Literal::new_lang_unchecked(
                        lex,
                        rest[tag_index + 1..].to_owned(),
                    )));
                }
                // datatype
                let mut dt_split = rest.split("^^");
                dt_split.next(); // empty
                match dt_split.next() {
                    Some(dt) => {
                        let unquoted = dt[1..dt.len() - 1].to_owned().into_boxed_str();
                        let dt = Iri::<Box<str>>::new_unchecked(unquoted);
                        Ok(Term::<T>::from(Literal::new_dt(lex, dt)))
                    }
                    None => Err(TermError::UnsupportedDatatype(s.to_owned())),
                }
            }
        },
        Some('_') => Term::<T>::new_bnode(s[2..].to_owned()),
        _ => Term::<T>::new_iri(s.to_owned()),
    }
}

// transforms string triples into a sophia TripleSource
fn triple_source<'s, T: Data>(
    triples: impl Iterator<Item = (String, String, String)> + 's,
) -> GTripleSource<'s, HdtGraph<T>> {
    Box::new(
        triples
            .map(|(s, p, o)| -> Result<_> {
                debug_assert_ne!("", s, "triple_source subject is empty   ({s}, {p}, {o})");
                debug_assert_ne!("", p, "triple_source predicate is empty ({s}, {p}, {o})");
                debug_assert_ne!("", o, "triple_source object is empty    ({s}, {p}, {o})");
                Ok(StreamedTriple::by_value([auto_term(&s)?, auto_term(&p)?, auto_term(&o)?]))
            })
            .filter_map(|r| r.map_err(|e| error!("{e}")).ok())
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

impl<T: Data> Graph for HdtGraph<T> {
    type Triple = ByValue<[Term<T>; 3]>;
    type Error = Infallible; // infallible for now, figure out what to put here later

    fn triples(&self) -> GTripleSource<Self> {
        debug!("Iterating through ALL triples in the HDT Graph. This can be inefficient for large graphs.");
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
        let ss = term_string(s);
        let ps = term_string(p);
        let sid = self.hdt.dict.string_to_id(&ss, &IdKind::Subject);
        let pid = self.hdt.dict.string_to_id(&ps, &IdKind::Predicate);
        let s = auto_term(&ss).unwrap();
        let p = auto_term(&ps).unwrap();
        Box::new(
            BitmapIter::with_pattern(&self.hdt.triples, &TripleId::new(sid, pid, 0))
                .map(move |tid| -> Result<_> {
                    Ok(StreamedTriple::by_value([
                        s.clone(),
                        p.clone(),
                        auto_term(&self.hdt.dict.id_to_string(tid.object_id, &IdKind::Object).unwrap())?,
                    ]))
                })
                .filter_map(|r| r.map_err(|e| error!("{e}")).ok())
                .into_triple_source(),
        )
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
        let ps = term_string(p);
        let os = term_string(o);
        let pid = self.hdt.dict.string_to_id(&ps, &IdKind::Predicate);
        let oid = self.hdt.dict.string_to_id(&os, &IdKind::Object);
        // predicate can be neither a literal nor a blank node
        let p = Term::new_iri_unchecked(ps);
        let o = auto_term(&os).unwrap();
        Box::new(
            PredicateObjectIter::new(&self.hdt.triples, pid, oid)
                .map(move |sid| -> Result<_> {
                    Ok(StreamedTriple::by_value([
                        // subject is never a literal, so we can save a lot of CPU time here by not using auto_term
                        // TODO: could subject be a blank node?
                        Term::new_iri_unchecked(self.hdt.dict.id_to_string(sid, &IdKind::Subject).unwrap()),
                        p.clone(),
                        o.clone(),
                    ]))
                })
                .filter_map(|r| r.map_err(|e| error!("{e}")).ok())
                .into_triple_source(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use sophia::triple::Triple;
    use std::fs::File;
    use std::rc::Rc;

    #[test]
    fn test_graph() {
        type T = Rc<str>;
        init();
        let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
        let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
        let graph = HdtGraph::<Rc<str>>::new(hdt);
        let triples: Vec<_> = graph.triples().collect();
        assert_eq!(triples.len(), 327);
        assert!(graph
            .triples_with_s(&Term::<T>::new_iri_unchecked("http://www.snik.eu/ontology/meta".to_owned()))
            .next()
            .is_some());
        for uri in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let term = Term::<T>::new_iri_unchecked(uri.to_owned());
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
        let s = Term::<T>::new_iri_unchecked("http://www.snik.eu/ontology/meta/Top".to_owned());
        let p = Term::<T>::new_iri_unchecked("http://www.w3.org/2000/01/rdf-schema#label".to_owned());
        let o = Term::<T>::from(Literal::new_lang_unchecked("top class", "en"));
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
        assert!(graph.triples_with_o(&Term::<T>::from("22.10".to_owned())).count() == 1);
        let date = &Term::<T>::from(Literal::new_dt(
            "2022-10-20",
            Iri::<&str>::new_unchecked("http://www.w3.org/2001/XMLSchema#date"),
        ));
        assert!(graph.triples_with_o(date).count() == 1);
        // not in snik meta but only in local test file to make sure explicit xsd:string works
        /*
        let testo = &Term::<T>::from(Literal::new_dt(
            "testo",
            Iri::<&str>::new_unchecked("http://www.w3.org/2001/XMLSchema#string"),
        ));
        assert!(graph.triples_with_o(testo).count() == 1);
        */
    }
}

// //! *This module is available only if HDT is built with the `"sophia"` feature, included by default.*
use crate::four_sect_dict::IdKind;
use crate::hdt::Hdt;
use crate::triples::{Id, ObjectIter, PredicateIter, PredicateObjectIter, SubjectIter};
use log::debug;
use sophia::api::graph::Graph;
use sophia::api::term::matcher::TermMatcher;
use sophia::api::term::{BnodeId, IriRef, LanguageTag, Term};
use std::convert::Infallible;
use std::io::{self, Error, ErrorKind};
use std::iter;
use std::sync::Arc;

mod term;
pub use term::HdtTerm;

/// HdtGraph does not support all of the Sophia TermMatcher functionality.
enum HdtMatcher {
    Constant((HdtTerm, Id)),
    Other,
}

fn id_term(hdt: &Hdt, id: Id, kind: IdKind) -> HdtTerm {
    auto_term(&hdt.dict.id_to_string(id, kind).unwrap()).unwrap()
    // TODO: optimize by excluding cases depending on the id kind
    //IriRef::new_unchecked(MownStr::from(s)).into_term()
}

/// Transforms a Sophia TermMatcher to a constant HdtTerm and Id if possible.
/// Returns none if it matches a constant term that cannot be found.
fn unpack_matcher<T: TermMatcher>(hdt: &Hdt, tm: &T, kind: IdKind) -> Option<HdtMatcher> {
    match tm.constant() {
        Some(t) => match HdtTerm::try_from(t.borrow_term()) {
            Some(t) => {
                let id = hdt.dict.string_to_id(&term_string(&t), kind);
                if id == 0 {
                    return None;
                }
                Some(HdtMatcher::Constant((t, id)))
            }
            None => None,
        },
        None => Some(HdtMatcher::Other),
    }
}

/// Create the correct Sophia term for a given resource string.
/// Slow, use the appropriate method if you know which type (Literal, URI, or blank node) the string has.
fn auto_term(s: &str) -> io::Result<HdtTerm> {
    match s.chars().next() {
        None => Err(Error::new(ErrorKind::InvalidData, "empty input")),
        Some('"') => match s.rfind('"') {
            None => Err(Error::new(
                ErrorKind::InvalidData,
                format!("missing right quotation mark in literal string {s}"),
            )),
            Some(index) => {
                let lex = Arc::from(&s[1..index]);
                let rest = &s[index + 1..];
                // literal with no language tag and no datatype
                if rest.is_empty() {
                    return Ok(HdtTerm::LiteralDatatype(lex, term::XSD_STRING.clone()));
                }
                // either language tag or datatype
                if let Some(tag_index) = rest.find('@') {
                    let tag = LanguageTag::new_unchecked(Arc::from(&rest[tag_index + 1..]));
                    return Ok(HdtTerm::LiteralLanguage(lex, tag));
                }
                // datatype
                let mut dt_split = rest.split("^^");
                dt_split.next(); // empty
                dt_split.next().map_or_else(
                    || Err(Error::new(ErrorKind::InvalidData, format!("empty datatype in {s}"))),
                    |dt| {
                        let unquoted = &dt[1..dt.len() - 1];
                        let dt = IriRef::new_unchecked(Arc::from(unquoted));
                        Ok(HdtTerm::LiteralDatatype(lex, dt))
                    },
                )
            }
        },
        Some('_') => Ok(HdtTerm::BlankNode(BnodeId::new_unchecked(Arc::from(&s[2..])))),
        _ => Ok(HdtTerm::Iri(IriRef::new_unchecked(Arc::from(s)))),
    }
}

// Convert a SimpleTerm into the HDT String format.
// Sophia doesn't include the _: prefix for blank node strings but HDT expects it
// not needed for property terms, as they can't be blank nodes
fn term_string(t: &HdtTerm) -> String {
    match t {
        HdtTerm::BlankNode(b) => "_:".to_owned() + b.as_str(),
        HdtTerm::Iri(i) => i.as_str().to_owned(),
        HdtTerm::LiteralLanguage(l, lang) => {
            format!("\"{l}\"@{}", lang.as_str())
        }
        HdtTerm::LiteralDatatype(l, dt) => {
            let xsd_string: &str = "http://www.w3.org/2001/XMLSchema#string";
            let dts = dt.as_str();
            if dts == xsd_string { format!("\"{l}\"") } else { format!("\"{l}\"^^<{dts}>") }
        }
    }
}

impl Graph for Hdt {
    type Triple<'a> = [HdtTerm; 3];
    type Error = Infallible; // infallible for now, figure out what to put here later

    /// # Example
    /// ```
    /// use hdt::sophia::api::graph::Graph;
    /// fn print_first_triple(graph: hdt::Hdt) {
    ///     println!("{:?}", graph.triples().next().expect("no triple in the graph"));
    /// }
    /// ```
    fn triples(&self) -> impl Iterator<Item = Result<Self::Triple<'_>, Self::Error>> {
        debug!("Iterating through ALL triples in the HDT Graph. This can be inefficient for large graphs.");
        self.triples_all().map(move |[s, p, o]| {
            Ok([auto_term(&s).unwrap(), HdtTerm::Iri(IriRef::new_unchecked(p)), auto_term(&o).unwrap()])
        })
    }

    /// Only supports constant and "any" matchers.
    /// Non-constant matchers are supposed to be "any" matchers.
    /// # Example
    /// Who was born in Leipzig?
    /// ```
    /// use hdt::Hdt;
    /// use hdt::sophia::api::graph::Graph;
    /// use hdt::sophia::api::term::{IriRef, SimpleTerm, matcher::Any};
    ///
    /// fn query(dbpedia: hdt::Hdt) {
    ///     let birth_place = SimpleTerm::Iri(IriRef::new_unchecked("http://www.snik.eu/ontology/birthPlace".into()));
    ///     let leipzig = SimpleTerm::Iri(IriRef::new_unchecked("http://www.snik.eu/resource/Leipzig".into()));
    ///     let persons = dbpedia.triples_matching(Any, Some(birth_place), Some(leipzig));
    /// }
    /// ```
    fn triples_matching<'s, S, P, O>(
        &'s self, sm: S, pm: P, om: O,
    ) -> impl Iterator<Item = Result<Self::Triple<'s>, Self::Error>> + 's
    where
        S: TermMatcher + 's,
        P: TermMatcher + 's,
        O: TermMatcher + 's,
    {
        use HdtMatcher::{Constant, Other};
        let Some(xso) = unpack_matcher(self, &sm, IdKind::Subject) else {
            return Box::new(iter::empty()) as Box<dyn Iterator<Item = _>>;
        };
        let Some(xpo) = unpack_matcher(self, &pm, IdKind::Predicate) else { return Box::new(iter::empty()) };
        let Some(xoo) = unpack_matcher(self, &om, IdKind::Object) else { return Box::new(iter::empty()) };
        // TODO: improve error handling
        match (xso, xpo, xoo) {
            //if SubjectIter::with_pattern(&self.triples, [s.1, p.1, o.1]).next().is_some() { // always true
            (Constant(s), Constant(p), Constant(o)) => Box::new(iter::once(Ok([s.0, p.0, o.0]))),
            (Constant(s), Constant(p), Other) => Box::new(
                SubjectIter::with_pattern(&self.triples, [s.1, p.1, 0])
                    .map(|tid| auto_term(&self.dict.id_to_string(tid[2], IdKind::Object).unwrap()).unwrap())
                    .filter(move |term| om.matches(term))
                    .map(move |term| Ok([s.0.clone(), p.0.clone(), term])),
            ),
            (Constant(s), Other, Constant(o)) => Box::new(
                SubjectIter::with_pattern(&self.triples, [s.1, 0, o.1])
                    .map(|t| id_term(self, t[1], IdKind::Predicate))
                    .filter(move |term| pm.matches(term))
                    .map(move |term| Ok([s.0.clone(), term, o.0.clone()])),
            ),
            (Constant(s), Other, Other) => Box::new(
                SubjectIter::with_pattern(&self.triples, [s.1, 0, 0])
                    .map(move |t| [id_term(self, t[1], IdKind::Predicate), id_term(self, t[2], IdKind::Object)])
                    .filter(move |[pt, ot]| pm.matches(pt) && om.matches(ot))
                    .map(move |[pt, ot]| Ok([s.0.clone(), pt, ot])),
            ),
            (Other, Constant(p), Constant(o)) => Box::new(
                PredicateObjectIter::new(&self.triples, p.1, o.1)
                    .map(|sid| id_term(self, sid, IdKind::Subject))
                    .filter(move |term| sm.matches(term))
                    .map(move |term| Ok([term, p.0.clone(), o.0.clone()])),
            ),
            (Other, Constant(p), Other) => Box::new(
                PredicateIter::new(&self.triples, p.1)
                    .map(move |t| [id_term(self, t[0], IdKind::Subject), id_term(self, t[2], IdKind::Object)])
                    .filter(move |[st, ot]| sm.matches(st) && om.matches(ot))
                    .map(move |[st, ot]| Ok([st, p.0.clone(), ot])),
            ),
            (Other, Other, Constant(o)) => Box::new(ObjectIter::new(&self.triples, o.1).map(move |t| {
                Ok([
                    auto_term(&Arc::from(self.dict.id_to_string(t[0], IdKind::Subject).unwrap())).unwrap(),
                    id_term(self, t[1], IdKind::Predicate),
                    o.0.clone(),
                ])
            })),
            (Other, Other, Other) => Box::new(
                self.triples_all()
                    .map(move |[s, p, o]| {
                        [auto_term(&s).unwrap(), HdtTerm::Iri(IriRef::new_unchecked(p)), auto_term(&o).unwrap()]
                    })
                    .filter(move |[st, pt, ot]| sm.matches(st) && pm.matches(pt) && om.matches(ot))
                    .map(Result::Ok),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use fs_err::File;
    use sophia::api::prelude::Triple;
    use sophia::api::term::matcher::Any;

    #[test]
    fn test_graph() -> color_eyre::Result<()> {
        init();
        let file = File::open("tests/resources/snikmeta.hdt")?;
        let graph = Hdt::read(std::io::BufReader::new(file))?;
        // Hdt has a triples() method as well
        let triples: Vec<Result<[HdtTerm; 3], Infallible>> = graph.triples().collect();
        assert_eq!(triples.len(), 328);
        let meta_top = "http://www.snik.eu/ontology/meta/Top";
        assert!(
            graph
                .triples_matching(
                    Some(HdtTerm::Iri(IriRef::new_unchecked(Arc::from("http://www.snik.eu/ontology/meta")))),
                    Any,
                    Any
                )
                .next()
                .is_some()
        );
        for uri in [meta_top, "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let term = HdtTerm::Iri(IriRef::new_unchecked(Arc::from(uri)));
            let filtered: Vec<_> = triples
                .iter()
                .map(|triple| triple.as_ref().unwrap())
                .filter(|triple| triple.s().iri().is_some() && triple.s().iri().unwrap().to_string() == uri)
                .collect();
            let with_s: Vec<_> = graph.triples_matching(Some(term), Any, Any).map(Result::unwrap).collect();
            // Sophia strings can't be compared directly, use the Debug trait for string comparison that is more brittle and less elegant
            // could break in the future e.g. because of ordering
            let filtered_string = format!("{filtered:?}");
            let with_s_string = format!("{with_s:?}");
            assert_eq!(
                filtered_string, with_s_string,
                "different results between triples() and triples_with_s() for {uri}"
            );
        }
        let s = HdtTerm::Iri(IriRef::new_unchecked(meta_top.into()));
        let label = HdtTerm::Iri(IriRef::new_unchecked("http://www.w3.org/2000/01/rdf-schema#label".into()));
        let o = HdtTerm::LiteralLanguage("top class".into(), LanguageTag::new_unchecked("en".into()));
        assert!(graph.triples_matching(Any, Any, [o.borrow_term()]).next().is_some());

        let tvec = vec![[s.clone(), label.clone(), o.clone()]];
        assert_eq!(
            tvec,
            graph
                .triples_matching([s.borrow_term()], [label.borrow_term()], Any)
                .map(Result::unwrap)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            tvec,
            graph
                .triples_matching([s.borrow_term()], Any, [o.borrow_term()])
                .map(Result::unwrap)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            tvec,
            graph
                .triples_matching(Any, [label.borrow_term()], [o.borrow_term()])
                .map(Result::unwrap)
                .collect::<Vec<_>>()
        );
        assert_eq!(1, graph.triples_matching(Any, Any, ["22.10"]).count());
        let date = HdtTerm::LiteralDatatype(
            "2022-10-20".into(),
            IriRef::new_unchecked("http://www.w3.org/2001/XMLSchema#date".into()),
        );
        assert_eq!(1, graph.triples_matching(Any, Any, Some(&date)).count());
        // *** matchers other than constant and Any ********************************************
        let meta = HdtTerm::Iri(IriRef::new_unchecked("http://www.snik.eu/ontology/meta".into()));
        let modified = HdtTerm::Iri(IriRef::new_unchecked("http://purl.org/dc/terms/modified".into()));
        // SPO
        assert_eq!(2, graph.triples_matching([&meta, &s], [&label, &modified], [&date, &o]).count());
        // SP?
        assert_eq!(3, graph.triples_matching([&meta, &s], [&label, &modified], Any).count());
        // S?O
        assert_eq!(2, graph.triples_matching([&meta, &s], Any, [&date, &o]).count());
        // S??
        assert_eq!(
            graph.triples_matching([&meta, &s], Any, Any).count(),
            graph.triples_matching([&meta], Any, Any).count() + graph.triples_matching([&s], Any, Any).count(),
        );
        // ?P?
        assert_eq!(2, graph.triples_matching(Any, Any, [&date, &o]).count());
        // ?PO
        assert_eq!(2, graph.triples_matching(Any, [&label, &modified], [&date, &o]).count());
        // ?P?
        assert_eq!(
            graph.triples_matching(Any, [&label, &modified], Any).count(),
            graph.triples_matching(Any, [&label], Any).count()
                + graph.triples_matching(Any, [&modified], Any).count()
        );
        // test for errors involving blank nodes
        let blank = HdtTerm::BlankNode(BnodeId::new_unchecked("b1".into()));
        // blank node as input
        assert_eq!(3, graph.triples_matching(Some(&blank), Any, Any).count());
        assert_eq!(1, graph.triples_matching(Any, Any, Some(&blank)).count());
        // blank node as output
        let rdftype =
            HdtTerm::Iri(IriRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type".into()));
        let owlrestriction =
            HdtTerm::Iri(IriRef::new_unchecked("http://www.w3.org/2002/07/owl#Restriction".into()));
        assert_eq!(1, graph.triples_matching(Any, Some(rdftype), Some(owlrestriction)).count());
        // not in the original SNIK meta but added to cover more cases
        let s = HdtTerm::Iri(IriRef::new_unchecked("http://www.snik.eu/ontology/meta/хобби-N-0".into()));
        let o = HdtTerm::LiteralLanguage("ХОББИ".into(), LanguageTag::new_unchecked("ru".into()));
        let tvec = vec![[s.clone(), label.clone(), o.clone()]];
        assert_eq!(
            tvec,
            graph
                .triples_matching([s.borrow_term()], [label.borrow_term()], Any)
                .map(Result::unwrap)
                .collect::<Vec<_>>()
        );
        Ok(())
    }
}

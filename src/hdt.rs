use crate::containers::ControlInfo;
use crate::four_sect_dict::{DictErr, IdKind};
use crate::header::Header;
use crate::predicate_object_iter::PredicateObjectIter;
use crate::triples::{BitmapIter, TripleId, TriplesBitmap};
use crate::FourSectDict;
use bytesize::ByteSize;
use log::{debug, error};
use std::io;
use std::marker::PhantomData;
use thiserror::Error;

/// In-memory representation of an RDF graph loaded from an HDT file.
/// Allows queries by triple patterns.
#[derive(Debug)]
pub struct Hdt<T: Data> {
    //global_ci: ControlInfo,
    //header: Header,
    /// in-memory representation of dictionary
    pub dict: FourSectDict,
    /// in-memory representation of triples
    pub triples: TriplesBitmap,
    phantom: PhantomData<T>,
}

/// Type of String pointer to return, intended for Box, Rc and Arc.
pub trait Data:
    AsRef<str> + Clone + Eq + From<std::boxed::Box<str>> + From<String> + From<&'static str> + std::hash::Hash
{
}
impl<T> Data for T where
    T: AsRef<str> + Clone + Eq + From<std::boxed::Box<str>> + From<String> + From<&'static str> + std::hash::Hash
{
}

type StringTriple<T> = (T, T, T);

/// The error type for the `translate_id` method.
#[derive(Error, Debug)]
#[error("Cannot translate triple ID {t:?} to string triple: {e}")]
pub struct TranslateErr {
    #[source]
    e: DictErr,
    t: TripleId,
}

impl<T: Data> Hdt<T> {
    /// Creates an immutable HDT instance containing the dictionary and triples from the given reader.
    /// The reader must point to the beginning of the data of an HDT file as produced by hdt-cpp.
    /// FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order is the only supported implementation.
    /// The format is specified at <https://www.rdfhdt.org/hdt-binary-format/>, however there are some deviations.
    /// The initial HDT specification at <http://www.w3.org/Submission/2011/03/> is outdated and not supported.
    pub fn new<R: std::io::BufRead>(mut reader: R) -> io::Result<Self> {
        ControlInfo::read(&mut reader)?;
        Header::read(&mut reader)?;
        let mut dict = FourSectDict::read(&mut reader)?;
        let triples = TriplesBitmap::read_sect(&mut reader)?;
        dict.validate()?;
        let hdt = Hdt { dict, triples, phantom: PhantomData {} };
        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        //debug!("{hdt:#?}");
        Ok(hdt)
    }

    /// Recursive size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.dict.size_in_bytes() + self.triples.size_in_bytes()
    }

    /// Don't use this for many triples with shared values as you won't benefit from deduplication if you are using a smartpointer like Rc<str> or Arc<str>.
    fn translate_id(&self, t: TripleId) -> Result<StringTriple<T>, TranslateErr> {
        let s = self.dict.id_to_string(t.subject_id, &IdKind::Subject).map_err(|e| TranslateErr { e, t })?;
        let p = self.dict.id_to_string(t.predicate_id, &IdKind::Predicate).map_err(|e| TranslateErr { e, t })?;
        let o = self.dict.id_to_string(t.object_id, &IdKind::Object).map_err(|e| TranslateErr { e, t })?;
        Ok((s.into(), p.into(), o.into()))
    }

    /// An iterator visiting *all* triples as strings in order.
    /// Using this method with a filter can be inefficient for large graphs,
    /// because the strings are stored in compressed form and must be decompressed and allocated.
    /// Whenever possible, use [`Hdt::triples_with`] instead.
    pub fn triples(&self) -> impl Iterator<Item = StringTriple<T>> + '_ {
        self.triples.into_iter().map(|id| self.translate_id(id).unwrap())
    }

    /// An iterator visiting all triples for a given triple pattern with exactly two variables, i.e. S??, ?P? or ??O.
    /// Returns translated triples as strings.
    /// If the subject is given, you can also use [`BitmapIter::with_pattern`] with a `TripleId` where property and object are 0.
    /// Much more effient than filtering the result of [`Hdt::triples`].
    /// If you want to query triple patterns with only one variable, use `triples_with_sp` etc. instead.
    pub fn triples_with(&self, s: &str, kind: &'static IdKind) -> Box<dyn Iterator<Item = StringTriple<T>> + '_> {
        debug_assert_ne!("", s);
        let id = self.dict.string_to_id(s, kind);
        if id == 0 {
            return Box::new(std::iter::empty());
        }
        let owned = s.to_owned();
        Box::new(
            self.triples
                .triples_with_id(id, kind)
                .map(move |tid| self.translate_id(tid))
                .filter_map(move |r| r.map_err(|e| error!("Error on triple with {kind:?} {owned}: {e}")).ok()),
        )
    }

    // TODO extract common code out of triples_with_...

    /// Get all triples with the given subject and property.
    pub fn objects_with_sp(&self, s: &str, p: &str) -> Box<dyn Iterator<Item = String> + '_> {
        let sid = self.dict.string_to_id(s, &IdKind::Subject);
        let pid = self.dict.string_to_id(p, &IdKind::Predicate);
        if sid == 0 || pid == 0 {
            return Box::new(std::iter::empty());
        }
        let s_owned = s.to_owned();
        let p_owned = p.to_owned();
        Box::new(
            BitmapIter::with_pattern(&self.triples, &TripleId::new(sid, pid, 0))
                .map(move |tid| self.dict.id_to_string(tid.object_id, &IdKind::Object))
                .filter_map(move |r| {
                    r.map_err(|e| error!("Error on triple with subject {s_owned} and property {p_owned}: {e}"))
                        .ok()
                }),
        )
    }

    /// Get all triples with the given subject and property.
    /// Inefficient with large results because the subject and property strings are duplicated for each triple.
    /// Consider using [Self::objects_with_sp()] instead.
    pub fn triples_with_sp<'a>(&'a self, s: &'a str, p: &'a str) -> impl Iterator<Item = StringTriple<T>> + '_ {
        let st = T::from(s.to_owned());
        let pt = T::from(p.to_owned());
        self.objects_with_sp(s, p).map(move |o| (st.clone(), pt.clone(), T::from(o)))
    }

    /// Get all triples with the given subject and object.
    /// The current implementation queries all triple IDs for the given subject and filters them for the given object.
    /// This method is faster then filtering on translated strings but can be further optimized by creating a special SO iterator.
    pub fn triples_with_so(&self, s: &str, o: &str) -> Box<dyn Iterator<Item = StringTriple<T>> + '_> {
        let sid = self.dict.string_to_id(s, &IdKind::Subject);
        let oid = self.dict.string_to_id(o, &IdKind::Object);
        if sid == 0 || oid == 0 {
            return Box::new(std::iter::empty());
        }
        let st = T::from(s.to_owned());
        let ot = T::from(o.to_owned());
        Box::new(
            self.triples
                .triples_with_id(sid, &IdKind::Subject)
                .filter(move |tid| tid.object_id == oid)
                .map(move |tid| self.dict.id_to_string(tid.predicate_id, &IdKind::Predicate))
                .filter_map(move |r| {
                    //r.map_err(|e| error!("Error on triple with subject {st} and object {ot}: {e}")).ok()
                    r.map_err(|e| error!("Error on triple: {e}")).ok()
                })
                .map(move |ps| (st.clone(), T::from(ps), ot.clone())),
        )
    }

    /// Get all subjects with the given property and object.
    pub fn subjects_with_po(&self, p: &str, o: &str) -> Box<dyn Iterator<Item = String> + '_> {
        let pid = self.dict.string_to_id(p, &IdKind::Predicate);
        let oid = self.dict.string_to_id(o, &IdKind::Object);
        // predicate or object not in dictionary
        if pid == 0 || oid == 0 {
            return Box::new(std::iter::empty());
        }
        let p_owned = p.to_owned();
        let o_owned = o.to_owned();
        //let s = self.dict.id_to_string(t.subject_id, &IdKind::Subject).map_err(|e| TranslateErr { e, t })?;
        Box::new(
            PredicateObjectIter::new(&self.triples, pid, oid)
                .map(move |sid| self.dict.id_to_string(sid, &IdKind::Subject))
                .filter_map(move |r| {
                    r.map_err(|e| error!("Error on triple with property {p_owned} and object {o_owned}: {e}")).ok()
                }),
        )
    }

    /// Get all triples with the given property and object.
    /// Inefficient with large results because the property and object are duplicated for each triple.
    /// Consider using [Self::subjects_with_po()] instead.
    pub fn triples_with_po<'a>(&'a self, p: &'a str, o: &'a str) -> impl Iterator<Item = StringTriple<T>> + 'a {
        let pt = T::from(p.to_owned());
        let ot = T::from(o.to_owned());
        self.subjects_with_po(p, o).map(move |s| (T::from(s), pt.clone(), ot.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use pretty_assertions::{assert_eq, assert_ne};
    use std::fs::File;
    use std::rc::Rc;

    #[test]
    fn triples() {
        init();
        let filename = "tests/resources/snikmeta.hdt";
        let file = File::open(filename).expect("error opening file");
        let hdt = Hdt::<Rc<str>>::new(std::io::BufReader::new(file)).unwrap();
        let triples = hdt.triples();
        let v: Vec<StringTriple<_>> = triples.collect();
        assert_eq!(v.len(), 327);
        assert_ne!(0, hdt.dict.string_to_id("http://www.snik.eu/ontology/meta", &IdKind::Subject));
        for uri in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let filtered: Vec<_> = v.clone().into_iter().filter(|triple| triple.0.as_ref() == uri).collect();
            let with_s: Vec<_> = hdt.triples_with(uri, &IdKind::Subject).collect();
            assert_eq!(filtered, with_s, "different results between triples() and triples_with_s() for {}", uri);
        }
        let s = "http://www.snik.eu/ontology/meta/Top";
        let p = "http://www.w3.org/2000/01/rdf-schema#label";
        let o = "\"top class\"@en";
        let triple_vec = vec![(Rc::from(s), Rc::from(p), Rc::from(o))];
        assert_eq!(triple_vec, hdt.triples_with_sp(s, p).collect::<Vec<_>>());
        assert_eq!(triple_vec, hdt.triples_with_so(s, o).collect::<Vec<_>>());
        assert_eq!(triple_vec, hdt.triples_with_po(p, o).collect::<Vec<_>>());
        let et = "http://www.snik.eu/ontology/meta/EntityType";
        assert_eq!(4, hdt.subjects_with_po("http://www.w3.org/2000/01/rdf-schema#subClassOf", et).count());
        assert_eq!(
            12,
            hdt.triples_with("http://www.w3.org/2000/01/rdf-schema#subClassOf", &IdKind::Predicate).count()
        );
        assert_eq!(20, hdt.triples_with(et, &IdKind::Object).count());
    }
}

//! I define [`HdtTerm`], an implementation of [`sophia::api::term::Term`].
use sophia::api::ns::{rdf, xsd};
use sophia::api::term::{BnodeId, LanguageTag, Term, TermKind};
use sophia::api::MownStr;
use sophia::iri::IriRef;
use std::sync::Arc;

lazy_static::lazy_static! {
    pub static ref XSD_STRING: IriRef<Arc<str>> = xsd::string.iri().unwrap().map_unchecked(|m| Arc::from(m.as_ref()));
    pub static ref RDF_LANG_STRING: IriRef<Arc<str>> = rdf::langString.iri().unwrap().map_unchecked(|m| Arc::from(m.as_ref()));
}

/// An implementation of [`sophia::api::term::Term`] for [`HdtGraph`](super::HdtGraph).
#[derive(Clone, Debug)]
pub enum HdtTerm {
    /// This HdtTerm is an IRI
    Iri(IriRef<Arc<str>>),
    /// This HdtTerm is a blank node
    BlankNode(BnodeId<Arc<str>>),
    /// This HdtTerm is a literal with a "standard" datatype
    LiteralDatatype(Arc<str>, IriRef<Arc<str>>),
    /// This HdtTerm is a language string literal
    LiteralLanguage(Arc<str>, LanguageTag<Arc<str>>),
}

impl HdtTerm {
    /// Convert t into an HdtTerm if it is a supported kind of term.
    pub fn try_from<T: Term>(t: T) -> Option<HdtTerm> {
        match t.kind() {
            TermKind::Iri => t.iri().map(|iri| HdtTerm::Iri(iri.map_unchecked(mown2arc))),
            TermKind::BlankNode => t.bnode_id().map(|bnid| HdtTerm::BlankNode(bnid.map_unchecked(mown2arc))),
            TermKind::Literal => Some({
                let lex = mown2arc(t.lexical_form().unwrap());
                if let Some(tag) = t.language_tag() {
                    let tag = tag.map_unchecked(mown2arc);
                    HdtTerm::LiteralLanguage(lex, tag)
                } else {
                    let dt = t.datatype().unwrap().map_unchecked(mown2arc);
                    HdtTerm::LiteralDatatype(lex, dt)
                }
            }),
            _ => None,
        }
    }
}

impl Term for HdtTerm {
    type BorrowTerm<'x> = &'x Self where Self: 'x;

    fn kind(&self) -> TermKind {
        match self {
            HdtTerm::Iri(_) => TermKind::Iri,
            HdtTerm::BlankNode(_) => TermKind::BlankNode,
            HdtTerm::LiteralDatatype(_, _) => TermKind::Literal,
            HdtTerm::LiteralLanguage(_, _) => TermKind::Literal,
        }
    }

    fn borrow_term(&self) -> Self::BorrowTerm<'_> {
        self
    }

    fn iri(&self) -> Option<sophia::api::term::IriRef<mownstr::MownStr>> {
        match self {
            HdtTerm::Iri(iri) => Some(iri.as_ref().map_unchecked(MownStr::from_str)),
            _ => None,
        }
    }

    fn bnode_id(&self) -> Option<BnodeId<mownstr::MownStr>> {
        match self {
            HdtTerm::BlankNode(bnid) => Some(bnid.as_ref().map_unchecked(MownStr::from_str)),
            _ => None,
        }
    }

    fn lexical_form(&self) -> Option<mownstr::MownStr> {
        match self {
            HdtTerm::LiteralDatatype(lex, _) => Some(lex.as_ref().into()),
            HdtTerm::LiteralLanguage(lex, _) => Some(lex.as_ref().into()),
            _ => None,
        }
    }

    fn datatype(&self) -> Option<sophia::api::term::IriRef<mownstr::MownStr>> {
        match self {
            HdtTerm::LiteralDatatype(_, datatype) => Some(datatype.as_ref().map_unchecked(MownStr::from_str)),
            HdtTerm::LiteralLanguage(_, _) => rdf::langString.iri(),
            _ => None,
        }
    }

    fn language_tag(&self) -> Option<LanguageTag<mownstr::MownStr>> {
        match self {
            HdtTerm::LiteralLanguage(_, tag) => Some(tag.as_ref().map_unchecked(MownStr::from_str)),
            _ => None,
        }
    }
}

impl PartialEq for HdtTerm {
    fn eq(&self, other: &Self) -> bool {
        Term::eq(self, other)
    }
}

impl Eq for HdtTerm {}

fn mown2arc(m: MownStr) -> Arc<str> {
    Box::<str>::from(m.to_owned()).into()
}

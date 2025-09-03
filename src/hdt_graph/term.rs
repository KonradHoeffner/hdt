//! I define [`HdtTerm`], an implementation of [`sophia::api::term::Term`].
use sophia::api::MownStr;
use sophia::api::ns::{rdf, xsd};
use sophia::api::term::{BnodeId, LanguageTag, Term, TermKind};
use sophia::iri::IriRef;
use std::sync::{Arc, LazyLock};

pub static XSD_STRING: LazyLock<IriRef<Arc<str>>> =
    LazyLock::new(|| xsd::string.iri().unwrap().map_unchecked(|m| Arc::from(m.as_ref())));

/// An implementation of [`sophia::api::term::Term`] for HDT.
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
    #[allow(clippy::needless_pass_by_value)]
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
    type BorrowTerm<'x>
        = &'x Self
    where
        Self: 'x;

    fn kind(&self) -> TermKind {
        match self {
            HdtTerm::Iri(_) => TermKind::Iri,
            HdtTerm::BlankNode(_) => TermKind::BlankNode,
            HdtTerm::LiteralDatatype(..) | HdtTerm::LiteralLanguage(..) => TermKind::Literal,
        }
    }

    fn borrow_term(&self) -> Self::BorrowTerm<'_> {
        self
    }

    fn iri(&self) -> Option<sophia::api::term::IriRef<mownstr::MownStr<'_>>> {
        match self {
            HdtTerm::Iri(iri) => Some(iri.as_ref().map_unchecked(MownStr::from_ref)),
            _ => None,
        }
    }

    fn bnode_id(&self) -> Option<BnodeId<mownstr::MownStr<'_>>> {
        match self {
            HdtTerm::BlankNode(bnid) => Some(bnid.as_ref().map_unchecked(MownStr::from_ref)),
            _ => None,
        }
    }

    fn lexical_form(&self) -> Option<mownstr::MownStr<'_>> {
        match self {
            HdtTerm::LiteralDatatype(lex, _) | HdtTerm::LiteralLanguage(lex, _) => Some(lex.as_ref().into()),
            _ => None,
        }
    }

    fn datatype(&self) -> Option<sophia::api::term::IriRef<mownstr::MownStr<'_>>> {
        match self {
            HdtTerm::LiteralDatatype(_, datatype) => Some(datatype.as_ref().map_unchecked(MownStr::from_ref)),
            HdtTerm::LiteralLanguage(..) => rdf::langString.iri(),
            _ => None,
        }
    }

    fn language_tag(&self) -> Option<LanguageTag<mownstr::MownStr<'_>>> {
        match self {
            HdtTerm::LiteralLanguage(_, tag) => Some(tag.as_ref().map_unchecked(MownStr::from_ref)),
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
    Box::<str>::from(m).into()
}

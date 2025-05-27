use crate::containers::ControlInfo;
use crate::containers::rdf::{Id, Literal, Term, Triple};
use ntriple::parser::triple_line;
use std::collections::BTreeSet;
use std::io::BufRead;
use std::str;

/// Metadata about the dataset, see <https://www.rdfhdt.org/hdt-binary-format/#header>.
#[derive(Debug, Clone)]
pub struct Header {
    /// Header data format. Only "ntriples" is supported.
    pub format: String,
    /// The number of bytes of the header data.
    pub length: usize,
    /// Triples describing the dataset.
    pub body: BTreeSet<Triple>,
}

/// The error type for the `read` method.
#[derive(thiserror::Error, Debug)]
#[error("failed to read HDT header")]
pub enum HeaderReadError {
    #[error("{0}")]
    Other(String),
    Io(#[from] std::io::Error),
    ControlInfoError(#[from] crate::containers::ControlInfoReadError),
    #[error("invalid header format {0}, only 'ntriples' is supported")]
    InvalidHeaderFormat(String),
}

impl Header {
    /// Reader needs to be positioned directly after the global control information.
    pub fn read<R: BufRead>(reader: &mut R) -> Result<Self, HeaderReadError> {
        use HeaderReadError::*;
        let header_ci = ControlInfo::read(reader)?;
        if header_ci.format != "ntriples" {
            return Err(InvalidHeaderFormat(header_ci.format));
        }

        //let ls = header_ci.get("length").ok_or_else(|| "missing header length".to_owned().into())?;
        let ls = header_ci.get("length").unwrap();
        let length = ls.parse::<usize>().unwrap();
        //ls.parse::<usize>().map_err(|_| format!("invalid header length '{ls}'").into())?;

        let mut body_buffer: Vec<u8> = vec![0; length];
        reader.read_exact(&mut body_buffer)?;
        let mut body = BTreeSet::new();

        for line_slice in body_buffer.split(|b| b == &b'\n') {
            let line = str::from_utf8(line_slice).map_err(|_| Other("Header is not UTF-8".to_owned()))?;
            if let Ok(Some(triple)) = triple_line(line) {
                let subject = match triple.subject {
                    ntriple::Subject::IriRef(iri) => Id::Named(iri),
                    ntriple::Subject::BNode(id) => Id::Blank(id),
                };

                let ntriple::Predicate::IriRef(predicate) = triple.predicate;

                let object = match triple.object {
                    ntriple::Object::IriRef(iri) => Term::Id(Id::Named(iri)),
                    ntriple::Object::BNode(id) => Term::Id(Id::Blank(id)),
                    ntriple::Object::Lit(lit) => Term::Literal(match lit.data_type {
                        ntriple::TypeLang::Lang(lan) => Literal::new_lang(lit.data, lan),
                        ntriple::TypeLang::Type(data_type) => {
                            // workaround incorrect https in xsd prefix in ntriples dependency
                            if data_type == "http://www.w3.org/2001/XMLSchema#string"
                                || data_type == "https://www.w3.org/2001/XMLSchema#string"
                            {
                                Literal::new(lit.data)
                            } else {
                                Literal::new_typed(lit.data, data_type)
                            }
                        }
                    }),
                };

                body.insert(Triple::new(subject, predicate, object));
            }
        }
        Ok(Header { format: header_ci.format, length, body })
    }

    pub fn write(&self, write: &mut impl std::io::Write) -> Result<(), HeaderReadError> {
        ControlInfo::header().write(write)?;
        for mut triple in &self.body {
            writeln!(write, "{}", triple);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use std::fs::File;
    use std::io::BufReader;

    #[test]
    fn read_header() -> color_eyre::Result<()> {
        init();
        let file = File::open("tests/resources/yago_header.hdt")?;
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader)?;

        let header = Header::read(&mut reader)?;
        assert_eq!(header.format, "ntriples");
        assert_eq!(header.length, 1891);
        assert_eq!(header.body.len(), 22);
        Ok(())
    }
}

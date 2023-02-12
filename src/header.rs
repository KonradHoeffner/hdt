use crate::containers::rdf::{Id, Literal, Term, Triple};
use crate::containers::ControlInfo;
use ntriple::parser::triple_line;
use std::collections::BTreeSet;
use std::io;
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

impl Header {
    /// Reader needs to be positioned directly after the global control information.
    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;

        let header_ci = ControlInfo::read(reader)?;
        if header_ci.format != "ntriples" {
            return Err(Error::new(InvalidData, "Headers currently only support the NTriples format"));
        }

        let length = header_ci
            .get("length")
            .and_then(|v| v.parse::<usize>().ok())
            .ok_or_else(|| Error::new(InvalidData, "Header's length is missing or invalid"))?;

        let mut body_buffer: Vec<u8> = vec![0; length];
        reader.read_exact(&mut body_buffer)?;
        let mut body = BTreeSet::new();

        for line_slice in body_buffer.split(|b| b == &b'\n') {
            let line = str::from_utf8(line_slice).map_err(|_| Error::new(InvalidData, "Header is not UTF-8"))?;
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
                            if data_type == "http://www.w3.org/2001/XMLSchema#string" {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use std::fs::File;
    use std::io::BufReader;

    #[test]
    fn read_header() {
        init();
        let file = File::open("tests/resources/yago_header.hdt").expect("error opening file");
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader).expect("error reading control info");

        if let Ok(header) = Header::read(&mut reader) {
            assert_eq!(header.format, "ntriples");
            assert_eq!(header.length, 1891);
            assert_eq!(header.body.len(), 22);
        } else {
            panic!("Failed to read header");
        }
    }
}

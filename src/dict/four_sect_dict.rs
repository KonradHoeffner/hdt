use crate::dict::{DictSect, IdKind};
use std::io;
use std::io::BufRead;

#[derive(Debug, Clone)]
pub struct FourSectDict {
    pub shared: DictSect,
    pub subjects: DictSect,
    pub predicates: DictSect,
    pub objects: DictSect,
}

impl FourSectDict {
    pub fn id_to_string(&self, id: usize, id_kind: IdKind) -> String {
        let shared_size = self.shared.num_strings();
        match id_kind {
            IdKind::Subject => {
                if (id < shared_size) {
                //println!("shared {} {}",id, self.shared.id_to_string(id));
                    self.shared.id_to_string(id)
                } else {
                //println!("not shared {} {} {} {} {}",id, id - shared_size, self.subjects.id_to_string((id - shared_size) ), shared_size, self.objects.num_strings());
                    self.subjects.id_to_string(id - shared_size)
                    //self.subjects.id_to_string(id)
                }
            }
            IdKind::Predicate => self.predicates.id_to_string(id),
            IdKind::Object => {
                if (id < shared_size) {
                    self.shared.id_to_string(id)
                } else {
                    self.objects.id_to_string(id - shared_size)
                }
            }
        }
    }

    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        Ok(FourSectDict {
            shared: DictSect::read(reader)?,
            subjects: DictSect::read(reader)?,
            predicates: DictSect::read(reader)?,
            objects: DictSect::read(reader)?,
        })
    }
}

// Copyright (c) 2024-2025, Decisym, LLC

use oxrdf::NamedNodeRef;

pub const HDT_CONTAINER: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#HDTv1");
pub const VOID_TRIPLES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://rdfs.org/ns/void#triples");
pub const VOID_PROPERTIES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://rdfs.org/ns/void#properties");
pub const VOID_DISTINCT_SUBJECTS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://rdfs.org/ns/void#distinctSubjects");
pub const VOID_DISTINCT_OBJECTS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://rdfs.org/ns/void#distinctObjects");
pub const VOID_DATASET: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://rdfs.org/ns/void#Dataset");
pub const HDT_STATISTICAL_INFORMATION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#statisticalInformation");
pub const HDT_PUBLICATION_INFORMATION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#publicationInformation");
pub const HDT_FORMAT_INFORMATION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#formatInformation");
pub const HDT_DICTIONARY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionary");
pub const HDT_TRIPLES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#triples");
pub const DC_TERMS_FORMAT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/dc/terms/format");
pub const HDT_NUM_TRIPLES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#triplesnumTriples");
pub const HDT_TRIPLES_ORDER: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#triplesOrder");
pub const HDT_ORIGINAL_SIZE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#originalSize");
pub const HDT_SIZE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#hdtSize");
pub const DC_TERMS_ISSUED: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/dc/terms/issued");
pub const HDT_DICT_SHARED_SO: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionarynumSharedSubjectObject");
pub const HDT_DICT_MAPPING: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionarymapping");
pub const HDT_DICT_SIZE_STRINGS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionarysizeStrings");
pub const HDT_DICT_BLOCK_SIZE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionaryblockSize");
pub const HDT_TYPE_BITMAP: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#triplesBitmap");
pub const HDT_DICTIONARY_TYPE_FOUR: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://purl.org/HDT/hdt#dictionaryFour");

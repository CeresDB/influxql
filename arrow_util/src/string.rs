use arrow::array::ArrayDataBuilder;
use arrow::array::StringArray;
use arrow::buffer::Buffer;
use arrow::buffer::NullBuffer;
use num_traits::{AsPrimitive, FromPrimitive, Zero};
use std::fmt::Debug;
use std::ops::Range;

/// A packed string array that stores start and end indexes into
/// a contiguous string slice.
///
/// The type parameter K alters the type used to store the offsets
#[derive(Debug, Clone)]
pub struct PackedStringArray<K> {
    /// The start and end offsets of strings stored in storage
    offsets: Vec<K>,
    /// A contiguous array of string data
    storage: String,
}

impl<K: Zero> Default for PackedStringArray<K> {
    fn default() -> Self {
        Self {
            offsets: vec![K::zero()],
            storage: String::new(),
        }
    }
}

impl<K: AsPrimitive<usize> + FromPrimitive + Zero> PackedStringArray<K> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_empty(len: usize) -> Self {
        Self {
            offsets: vec![K::zero(); len + 1],
            storage: String::new(),
        }
    }

    pub fn with_capacity(keys: usize, values: usize) -> Self {
        let mut offsets = Vec::with_capacity(keys + 1);
        offsets.push(K::zero());

        Self {
            offsets,
            storage: String::with_capacity(values),
        }
    }

    /// Append a value
    ///
    /// Returns the index of the appended data
    pub fn append(&mut self, data: &str) -> usize {
        let id = self.offsets.len() - 1;

        let offset = self.storage.len() + data.len();
        let offset = K::from_usize(offset).expect("failed to fit into offset type");

        self.offsets.push(offset);
        self.storage.push_str(data);

        id
    }

    /// Extends this [`PackedStringArray`] by the contents of `other`
    pub fn extend_from(&mut self, other: &PackedStringArray<K>) {
        let offset = self.storage.len();
        self.storage.push_str(other.storage.as_str());
        // Copy offsets skipping the first element as this string start delimiter is already
        // provided by the end delimiter of the current offsets array
        self.offsets.extend(
            other
                .offsets
                .iter()
                .skip(1)
                .map(|x| K::from_usize(x.as_() + offset).expect("failed to fit into offset type")),
        )
    }

    /// Extends this [`PackedStringArray`] by `range` elements from `other`
    pub fn extend_from_range(&mut self, other: &PackedStringArray<K>, range: Range<usize>) {
        let first_offset: usize = other.offsets[range.start].as_();
        let end_offset: usize = other.offsets[range.end].as_();

        let insert_offset = self.storage.len();

        self.storage
            .push_str(&other.storage[first_offset..end_offset]);

        self.offsets.extend(
            other.offsets[(range.start + 1)..(range.end + 1)]
                .iter()
                .map(|x| {
                    K::from_usize(x.as_() - first_offset + insert_offset)
                        .expect("failed to fit into offset type")
                }),
        )
    }

    /// Get the value at a given index
    pub fn get(&self, index: usize) -> Option<&str> {
        let start_offset = self.offsets.get(index)?.as_();
        let end_offset = self.offsets.get(index + 1)?.as_();

        Some(&self.storage[start_offset..end_offset])
    }

    /// Pads with empty strings to reach length
    pub fn extend(&mut self, len: usize) {
        let offset = K::from_usize(self.storage.len()).expect("failed to fit into offset type");
        self.offsets.resize(self.offsets.len() + len, offset);
    }

    /// Truncates the array to the given length
    pub fn truncate(&mut self, len: usize) {
        self.offsets.truncate(len + 1);
        let last_idx = self.offsets.last().expect("offsets empty");
        self.storage.truncate(last_idx.as_());
    }

    /// Removes all elements from this array
    pub fn clear(&mut self) {
        self.offsets.truncate(1);
        self.storage.clear();
    }

    pub fn iter(&self) -> PackedStringIterator<'_, K> {
        PackedStringIterator {
            array: self,
            index: 0,
        }
    }

    /// The number of strings in this array
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.len() == 1
    }

    /// Return the amount of memory in bytes taken up by this array
    pub fn size(&self) -> usize {
        self.storage.capacity() + self.offsets.capacity() * std::mem::size_of::<K>()
    }

    pub fn inner(&self) -> (&[K], &str) {
        (&self.offsets, &self.storage)
    }

    pub fn into_inner(self) -> (Vec<K>, String) {
        (self.offsets, self.storage)
    }
}

impl PackedStringArray<i32> {
    /// Convert to an arrow with an optional null bitmask
    pub fn to_arrow(&self, nulls: Option<NullBuffer>) -> StringArray {
        let len = self.offsets.len() - 1;
        let offsets = Buffer::from_slice_ref(&self.offsets);
        let values = Buffer::from(self.storage.as_bytes());

        let data = ArrayDataBuilder::new(arrow::datatypes::DataType::Utf8)
            .len(len)
            .add_buffer(offsets)
            .add_buffer(values)
            .nulls(nulls)
            .build()
            // TODO consider skipping the validation checks by using
            // `new_unchecked`
            .expect("Valid array data");
        StringArray::from(data)
    }
}

#[derive(Debug)]
pub struct PackedStringIterator<'a, K> {
    array: &'a PackedStringArray<K>,
    index: usize,
}

impl<'a, K: AsPrimitive<usize> + FromPrimitive + Zero> Iterator for PackedStringIterator<'a, K> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.array.get(self.index)?;
        self.index += 1;
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.array.len() - self.index;
        (len, Some(len))
    }
}

#[cfg(test)]
mod tests {
    use crate::string::PackedStringArray;

    #[test]
    fn test_storage() {
        let mut array = PackedStringArray::<i32>::new();

        array.append("hello");
        array.append("world");
        array.append("cupcake");

        assert_eq!(array.get(0).unwrap(), "hello");
        assert_eq!(array.get(1).unwrap(), "world");
        assert_eq!(array.get(2).unwrap(), "cupcake");
        assert!(array.get(-1_i32 as usize).is_none());

        assert!(array.get(3).is_none());

        array.extend(2);
        assert_eq!(array.get(3).unwrap(), "");
        assert_eq!(array.get(4).unwrap(), "");
        assert!(array.get(5).is_none());
    }

    #[test]
    fn test_empty() {
        let array = PackedStringArray::<u8>::new_empty(20);
        assert_eq!(array.get(12).unwrap(), "");
        assert_eq!(array.get(9).unwrap(), "");
        assert_eq!(array.get(3).unwrap(), "");
    }

    #[test]
    fn test_truncate() {
        let mut array = PackedStringArray::<i32>::new();

        array.append("hello");
        array.append("world");
        array.append("cupcake");

        array.truncate(1);
        assert_eq!(array.len(), 1);
        assert_eq!(array.get(0).unwrap(), "hello");

        array.append("world");
        assert_eq!(array.len(), 2);
        assert_eq!(array.get(0).unwrap(), "hello");
        assert_eq!(array.get(1).unwrap(), "world");
    }

    #[test]
    fn test_extend_from() {
        let mut a = PackedStringArray::<i32>::new();

        a.append("hello");
        a.append("world");
        a.append("cupcake");
        a.append("");

        let mut b = PackedStringArray::<i32>::new();

        b.append("foo");
        b.append("bar");

        a.extend_from(&b);

        let a_content: Vec<_> = a.iter().collect();
        assert_eq!(
            a_content,
            vec!["hello", "world", "cupcake", "", "foo", "bar"]
        );
    }

    #[test]
    fn test_extend_from_range() {
        let mut a = PackedStringArray::<i32>::new();

        a.append("hello");
        a.append("world");
        a.append("cupcake");
        a.append("");

        let mut b = PackedStringArray::<i32>::new();

        b.append("foo");
        b.append("bar");
        b.append("");
        b.append("fiz");

        a.extend_from_range(&b, 1..3);

        assert_eq!(a.len(), 6);

        let a_content: Vec<_> = a.iter().collect();
        assert_eq!(a_content, vec!["hello", "world", "cupcake", "", "bar", ""]);

        // Should be a no-op
        a.extend_from_range(&b, 0..0);

        let a_content: Vec<_> = a.iter().collect();
        assert_eq!(a_content, vec!["hello", "world", "cupcake", "", "bar", ""]);

        a.extend_from_range(&b, 0..1);

        let a_content: Vec<_> = a.iter().collect();
        assert_eq!(
            a_content,
            vec!["hello", "world", "cupcake", "", "bar", "", "foo"]
        );

        a.extend_from_range(&b, 1..4);

        let a_content: Vec<_> = a.iter().collect();
        assert_eq!(
            a_content,
            vec!["hello", "world", "cupcake", "", "bar", "", "foo", "bar", "", "fiz"]
        );
    }
}

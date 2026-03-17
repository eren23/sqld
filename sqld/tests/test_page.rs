use sqld::storage::{Page, PageType, PAGE_HEADER_SIZE, PAGE_SIZE, SLOT_SIZE};

// ---------------------------------------------------------------------------
// Construction & header
// ---------------------------------------------------------------------------

#[test]
fn new_page_defaults() {
    let page = Page::new(1, PageType::HeapData);
    assert_eq!(page.page_id(), 1);
    assert_eq!(page.page_type_raw(), PageType::HeapData as u16);
    assert_eq!(page.free_space_offset(), PAGE_SIZE as u16);
    assert_eq!(page.tuple_count(), 0);
    assert_eq!(page.flags(), 0);
    assert_eq!(page.lsn(), 0);
    assert!(page.verify_checksum());
}

#[test]
fn all_page_types() {
    let types = [
        PageType::HeapData,
        PageType::BtreeInternal,
        PageType::BtreeLeaf,
        PageType::HashBucket,
        PageType::Overflow,
        PageType::FreeSpaceMap,
    ];
    for pt in types {
        let page = Page::new(1, pt);
        assert_eq!(page.page_type_enum(), Some(pt));
        assert!(page.verify_checksum());
    }
}

#[test]
fn initial_free_space() {
    let page = Page::new(1, PageType::HeapData);
    assert_eq!(page.free_space(), PAGE_SIZE - PAGE_HEADER_SIZE);
}

#[test]
fn set_flags_and_lsn() {
    let mut page = Page::new(1, PageType::HeapData);
    page.set_flags(0xCAFE);
    page.set_lsn(999_999);
    assert_eq!(page.flags(), 0xCAFE);
    assert_eq!(page.lsn(), 999_999);
    assert!(page.verify_checksum());
}

// ---------------------------------------------------------------------------
// Tuple insert / fetch
// ---------------------------------------------------------------------------

#[test]
fn insert_single_tuple() {
    let mut page = Page::new(1, PageType::HeapData);
    let data = b"hello";
    let slot = page.insert_tuple(data).unwrap();
    assert_eq!(slot, 0);
    assert_eq!(page.tuple_count(), 1);
    assert_eq!(page.fetch_tuple(0).unwrap(), data.as_slice());
    assert!(page.verify_checksum());
}

#[test]
fn insert_multiple_tuples() {
    let mut page = Page::new(1, PageType::HeapData);
    for i in 0u8..10 {
        let data = vec![i; 50];
        let slot = page.insert_tuple(&data).unwrap();
        assert_eq!(slot, i as u16);
    }
    assert_eq!(page.tuple_count(), 10);
    for i in 0u8..10 {
        assert_eq!(page.fetch_tuple(i as u16).unwrap(), vec![i; 50].as_slice());
    }
    assert!(page.verify_checksum());
}

#[test]
fn free_space_accounting() {
    let mut page = Page::new(1, PageType::HeapData);
    let initial = page.free_space();

    page.insert_tuple(&[0xAA; 100]).unwrap();
    assert_eq!(page.free_space(), initial - 100 - SLOT_SIZE);

    page.insert_tuple(&[0xBB; 200]).unwrap();
    assert_eq!(page.free_space(), initial - 100 - 200 - 2 * SLOT_SIZE);
}

#[test]
fn insert_zero_length_rejected() {
    let mut page = Page::new(1, PageType::HeapData);
    assert!(page.insert_tuple(&[]).is_err());
}

#[test]
fn insert_until_full() {
    let mut page = Page::new(1, PageType::HeapData);
    let chunk = [0xFF; 500];
    let mut count = 0u16;
    loop {
        match page.insert_tuple(&chunk) {
            Ok(slot) => {
                assert_eq!(slot, count);
                count += 1;
            }
            Err(_) => break,
        }
    }
    // Usable space ≈ 8168 bytes. Each tuple costs 504 bytes. Expect ~16 tuples.
    assert!(count >= 15);
    assert!(page.verify_checksum());
}

#[test]
fn oversized_tuple_rejected() {
    let mut page = Page::new(1, PageType::HeapData);
    let too_big = vec![0u8; PAGE_SIZE]; // bigger than usable area
    assert!(page.insert_tuple(&too_big).is_err());
}

// ---------------------------------------------------------------------------
// Tuple delete
// ---------------------------------------------------------------------------

#[test]
fn delete_tuple() {
    let mut page = Page::new(1, PageType::HeapData);
    page.insert_tuple(b"keep").unwrap();
    page.insert_tuple(b"remove").unwrap();

    page.delete_tuple(1).unwrap();

    assert_eq!(page.fetch_tuple(0).unwrap(), b"keep");
    assert!(page.fetch_tuple(1).is_err()); // deleted
    assert!(page.verify_checksum());
}

#[test]
fn double_delete_fails() {
    let mut page = Page::new(1, PageType::HeapData);
    page.insert_tuple(b"x").unwrap();
    page.delete_tuple(0).unwrap();
    assert!(page.delete_tuple(0).is_err());
}

#[test]
fn delete_out_of_range() {
    let mut page = Page::new(1, PageType::HeapData);
    assert!(page.delete_tuple(0).is_err());
    page.insert_tuple(b"x").unwrap();
    assert!(page.delete_tuple(5).is_err());
}

#[test]
fn fetch_out_of_range() {
    let page = Page::new(1, PageType::HeapData);
    assert!(page.fetch_tuple(0).is_err());
}

// ---------------------------------------------------------------------------
// Slot reuse
// ---------------------------------------------------------------------------

#[test]
fn slot_reuse_after_delete() {
    let mut page = Page::new(1, PageType::HeapData);
    let s0 = page.insert_tuple(b"aaa").unwrap();
    let s1 = page.insert_tuple(b"bbb").unwrap();
    let s2 = page.insert_tuple(b"ccc").unwrap();
    assert_eq!((s0, s1, s2), (0, 1, 2));

    page.delete_tuple(1).unwrap();

    // New insert reuses slot 1.
    let s_new = page.insert_tuple(b"ddd").unwrap();
    assert_eq!(s_new, 1);
    assert_eq!(page.tuple_count(), 3); // count unchanged
    assert_eq!(page.fetch_tuple(1).unwrap(), b"ddd");
}

#[test]
fn reused_slot_needs_less_free_space() {
    let mut page = Page::new(1, PageType::HeapData);
    page.insert_tuple(&[1; 100]).unwrap();
    page.insert_tuple(&[2; 100]).unwrap();

    let before = page.free_space();
    page.delete_tuple(0).unwrap();
    // Free space doesn't change because data is fragmented, not reclaimed.
    assert_eq!(page.free_space(), before);

    // Insert using 80 bytes into the reused slot — only costs data, no new slot.
    page.insert_tuple(&[3; 80]).unwrap();
    assert_eq!(page.free_space(), before - 80); // no SLOT_SIZE deducted
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

#[test]
fn compact_reclaims_space() {
    let mut page = Page::new(1, PageType::HeapData);
    page.insert_tuple(&[1; 300]).unwrap();
    page.insert_tuple(&[2; 300]).unwrap();
    page.insert_tuple(&[3; 300]).unwrap();

    let free_before = page.free_space();
    page.delete_tuple(0).unwrap();
    page.delete_tuple(2).unwrap();
    assert_eq!(page.free_space(), free_before); // fragmented

    page.compact();
    assert_eq!(page.free_space(), free_before + 600); // 300*2 reclaimed
    assert_eq!(page.fetch_tuple(1).unwrap(), &[2u8; 300]);
    assert!(page.verify_checksum());
}

#[test]
fn compact_preserves_all_live_tuples() {
    let mut page = Page::new(1, PageType::HeapData);
    for i in 0u8..8 {
        page.insert_tuple(&[i; 100]).unwrap();
    }
    // Delete every other tuple.
    for i in (0u16..8).step_by(2) {
        page.delete_tuple(i).unwrap();
    }

    page.compact();

    for i in 0u8..8 {
        if i % 2 == 0 {
            assert!(page.fetch_tuple(i as u16).is_err());
        } else {
            assert_eq!(page.fetch_tuple(i as u16).unwrap(), &[i; 100]);
        }
    }
    assert!(page.verify_checksum());
}

#[test]
fn compact_empty_page_is_noop() {
    let mut page = Page::new(1, PageType::HeapData);
    let free = page.free_space();
    page.compact();
    assert_eq!(page.free_space(), free);
    assert!(page.verify_checksum());
}

// ---------------------------------------------------------------------------
// Serialization round-trip
// ---------------------------------------------------------------------------

#[test]
fn from_bytes_round_trip() {
    let mut page = Page::new(42, PageType::BtreeLeaf);
    page.insert_tuple(b"leaf-key-1").unwrap();
    page.insert_tuple(b"leaf-key-2").unwrap();
    page.set_lsn(7777);
    page.set_flags(0x0001);

    let bytes = page.as_bytes().to_vec();
    let restored = Page::from_bytes(&bytes).unwrap();

    assert_eq!(restored.page_id(), 42);
    assert_eq!(restored.page_type_enum(), Some(PageType::BtreeLeaf));
    assert_eq!(restored.lsn(), 7777);
    assert_eq!(restored.flags(), 0x0001);
    assert_eq!(restored.tuple_count(), 2);
    assert_eq!(restored.fetch_tuple(0).unwrap(), b"leaf-key-1");
    assert_eq!(restored.fetch_tuple(1).unwrap(), b"leaf-key-2");
    assert!(restored.verify_checksum());
}

#[test]
fn from_bytes_wrong_size() {
    assert!(Page::from_bytes(&[0u8; 100]).is_err());
    assert!(Page::from_bytes(&[0u8; PAGE_SIZE + 1]).is_err());
}

// ---------------------------------------------------------------------------
// Checksum
// ---------------------------------------------------------------------------

#[test]
fn checksum_valid_on_new_page() {
    let page = Page::new(1, PageType::HeapData);
    assert!(page.verify_checksum());
}

#[test]
fn checksum_valid_after_mutations() {
    let mut page = Page::new(1, PageType::HeapData);
    page.insert_tuple(b"data").unwrap();
    assert!(page.verify_checksum());
    page.delete_tuple(0).unwrap();
    assert!(page.verify_checksum());
    page.compact();
    assert!(page.verify_checksum());
    page.set_lsn(42);
    assert!(page.verify_checksum());
    page.set_flags(0xFFFF);
    assert!(page.verify_checksum());
}

#[test]
fn checksum_detects_header_corruption() {
    let page = Page::new(1, PageType::HeapData);
    let bytes = page.as_bytes().to_vec();
    let mut corrupted = bytes.clone();
    corrupted[0] ^= 0x01; // flip a bit in page_id
    let bad = Page::from_bytes(&corrupted).unwrap();
    assert!(!bad.verify_checksum());
}

#[test]
fn checksum_detects_data_corruption() {
    let mut page = Page::new(1, PageType::HeapData);
    page.insert_tuple(b"important").unwrap();
    let bytes = page.as_bytes().to_vec();
    let mut corrupted = bytes.clone();
    corrupted[PAGE_SIZE - 1] ^= 0xFF;
    let bad = Page::from_bytes(&corrupted).unwrap();
    assert!(!bad.verify_checksum());
}

// ---------------------------------------------------------------------------
// Large tuples and edge cases
// ---------------------------------------------------------------------------

#[test]
fn maximum_single_tuple() {
    let mut page = Page::new(1, PageType::HeapData);
    // Max tuple = PAGE_SIZE - HEADER - one SLOT
    let max_len = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE;
    let data = vec![0xAB; max_len];
    let slot = page.insert_tuple(&data).unwrap();
    assert_eq!(slot, 0);
    assert_eq!(page.fetch_tuple(0).unwrap().len(), max_len);
    assert_eq!(page.free_space(), 0);
    assert!(page.verify_checksum());
}

#[test]
fn insert_after_compact_fills_gap() {
    let mut page = Page::new(1, PageType::HeapData);
    // Fill with 4 tuples of 2000 bytes each = 8000 + 16 slots = 8016. fits in 8168.
    page.insert_tuple(&[1; 2000]).unwrap();
    page.insert_tuple(&[2; 2000]).unwrap();
    page.insert_tuple(&[3; 2000]).unwrap();
    page.insert_tuple(&[4; 2000]).unwrap();

    // Can't fit another 2000-byte tuple.
    assert!(page.insert_tuple(&[5; 2000]).is_err());

    // Delete two, compact, now we have room.
    page.delete_tuple(0).unwrap();
    page.delete_tuple(2).unwrap();
    page.compact();

    let slot = page.insert_tuple(&[5; 2000]).unwrap();
    assert_eq!(slot, 0); // reused
    assert_eq!(page.fetch_tuple(slot).unwrap(), &[5u8; 2000]);
    assert!(page.verify_checksum());
}

meta:
  id: smol_kv
  title: smol kv file format
  tags:
    - database
  endian: le
  bit-endian: be
  imports:
    - varint
    - uvarint
seq:
  - id: meta_a
    size: 16384
    type: meta_block
  - id: meta_b
    size: 16384
    type: meta_block
  - id: data
    size: 16384
    type: data_block(16384)
    repeat: eos
types:
  meta_block:
    seq:
      - id: magic
        contents: DICT
      - id: fields
        type: field
        repeat: until
        repeat-until: _.key == meta::end
      - id: checksum
        type: u4
      - id: freespace
        size-eos: true
    types:
      field:
        seq:
          - id: tag
            type: varint
          - id: val
            if: key != meta::end
            type:
              switch-on: key
              cases:
                "meta::entry": entry_val
                "meta::freelist": freelist_val
                _: uvarint
        instances:
          key:
            value: "tag.val < 0 ? -tag.val: tag.val"
            enum: meta
      entry_val:
        seq:
          - id: len
            type: uvarint
          - id: val
            size: len.val
            type: page
      freelist_val:
        seq:
          - id: len
            type: uvarint
          - id: val
            size: len.val
            type: freelist
    instances:
      freespace_size:
        value: freespace.size
  data_block:
    params:
      - id: block_size
        type: u4
    seq:
      - id: page
        if: length != 0
        type: page
    instances:
      length:
        pos: 2
        type: u2
      freespace:
        pos: length + 4
        size: block_size - 8 - length
      freespace_size:
        value: freespace.size
      checksum:
        pos: block_size - 4
        type: u4
  page:
    seq:
      - id: overflow_body
        if: count == 0 and is_leaf == false
        type: overflow_body_page(length)
      - id: overflow_tail
        if: count == 0 and is_leaf == true
        type: overflow_tail_page(length)
      - id: bptree_branch
        if: count != 0 and is_leaf == false
        type: bptree_branch_page(count)
      - id: bptree_leaf
        if: count != 0 and is_leaf == true
        type: bptree_leaf_page(count)
    instances:
      tag:
        pos: 0
        type: u2
        valid: 
          expr: _ < 0x8000
      count:
        value: tag & 0x3FFF
      is_leaf:
        value: (tag & 0x4000) == 0
      length:
        pos: 2
        type: u2
  overflow_body_page:
    params:
      - id: length
        type: u2
    instances:
      next_id:
        pos: 4
        type: u4
      payload:
        pos: 8
        size: length - 4
  overflow_tail_page:
    params:
      - id: length
        type: u2
    instances:
      payload:
        pos: 4
        size: length
  bptree_branch_page:
    params:
      - id: count
        type: u2
    instances:
      offsets:
        pos: 2
        type: u2
        repeat: expr
        repeat-expr: count + 1
      items:
        type: item(_index)
        repeat: expr
        repeat-expr: count
    types:
      item:
        params:
          - id: i
            type: s4
        instances:
          item:
            pos: _parent.offsets[i+1] + 4
            size: _parent.offsets[i] - _parent.offsets[i+1]
            type: pair
      pair:
        seq:
          - id: val
            type: u4
          - id: key
            size-eos: true
        instances:
          key_overflow:
            if: key.size > 3258
            pos: 4
            size-eos: true
            type: overflow_head
  bptree_leaf_page:
    params:
      - id: count
        type: u2
    instances:
      offsets:
        pos: 2
        type: u2
        repeat: expr
        repeat-expr: count + 1
      items:
        type: item(_index)
        repeat: expr
        repeat-expr: count
    types:
      item:
        params:
          - id: i
            type: s4
        instances:
          item:
            pos: _parent.offsets[i+1] +4
            size: _parent.offsets[i] - _parent.offsets[i+1]
            type: pair
      pair:
        seq:
          - id: klen
            type: uvarint
          - id: key
            size: klen.val
          - id: val
            size-eos: true
        instances:
          key_overflow:
            if: key.size > 3258
            pos: klen.len
            size: klen.val
            type: overflow_head
          val_overflow:
            if: val.size > 13092
            pos: klen.len + klen.val
            size-eos: true
            type: overflow_head
  freelist:
    seq:
      - id: tag
        contents: [0x00, 0x40]
      - id: length
        type: u2
      - id: next_id
        type: u4
      - id: free_ids
        type: u4
        repeat: expr
        repeat-expr: count
      - id: checksum
        type: u4
    instances:
      count:
        value: length / 4 - 1
  overflow_head:
    seq:
      - id: size
        type: uvarint
      - id: id
        type: u4
      - id: front
        size-eos: true
enums:
  meta:
    0: end
    1: version
    5: ckp
    6: update_time
    7: block_size
    8: block_count
    9: id
    10: prev_id
    11: free_recycled
    12: free_total
    13: freelist
    14: entry_size
    15: entry_id
    16: entry

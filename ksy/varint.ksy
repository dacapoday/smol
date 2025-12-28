meta:
  id: varint
  imports:
    - /common/vlq_base128_le
seq:
  - id: body
    type: vlq_base128_le
instances:
  len:
     value: body.len
  val:
    value: (body.value >> 1) ^ -(body.value & 1)

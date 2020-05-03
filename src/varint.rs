pub fn varint_length_packed(data: &[u8]) -> u32 {
    let mut i = 0;
    for _ in 0..data.len() {
        if (data[i] & 0x80) == 0 {
            break;
        }
        i += 1;
    }
    if i == data.len() { 0 } else { i as u32 + 1 }
}

pub fn varint_decode32(data: &[u8], value: &mut u32) -> usize {
    let len = varint_length_packed(&data[..data.len().min(5)]);
    let mut val = (data[0] & 0x7f) as u32;
    if len > 1 {
        val |= ((data[1] & 0x7f) as u32) << 7;
        if len > 2 {
            val |= ((data[2] & 0x7f) as u32) << 14;
            if len > 3 {
                val |= ((data[3] & 0x7f) as u32) << 21;
                if len > 4 {
                    val |= (data[4] as u32) << 28;
                }
            }
        }
    }
    *value = val;
    len as usize
}

pub fn varint_encode64(_bytes: &mut [u8], _value: i64) {
    unimplemented!()
}


pub fn varint_decode64(data: &[u8], value: &mut u64) -> usize {
    let len = varint_length_packed(&data[..data.len().min(10)]);
    if len < 5 {
        let mut tmp = 0;
        let tmp_len = varint_decode32(data, &mut tmp);
        *value = tmp as u64;
        return tmp_len;
    }
    let mut val: u64 = ((data[0] & 0x7f) as u64)
                 | (((data[1] & 0x7f) as u64) << 7)
                 | (((data[2] & 0x7f) as u64) << 14)
                 | (((data[3] & 0x7f) as u64) << 21);
    let mut shift = 28;
    for i in 4..len as usize {
        val |= ((data[i] & 0x7f) as u64) << shift;
        shift += 7;
    }
    *value = val;
    len as usize
}

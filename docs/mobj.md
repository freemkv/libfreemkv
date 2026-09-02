# MOBJ navigation command layout

`Cmd` is a decoded 12-byte HDMV navigation command. Field layout (big-endian):

- byte 0 = `op_cnt(3) grp(2) sub_grp(3)`
- byte 1 = `imm_op1(1) imm_op2(1) branch_opt(4)` (low nibble)
- byte 2 low nibble = `cmp_opt`
- byte 3 low 5 bits = `set_opt`
- then `dst`(u32) and `src`(u32)

import numpy as np
import pandas as pd

infile = open("MOD09GA.A2023048.h12v09.006.2023050045537.hdf", "rb")

header_bin = infile.read(4)
header_bin.hex()

def read_dd(file, offset=None):
    if offset:
        file.seek(offset)
    dd_tag = int.from_bytes(file.read(2))
    dd_ref = int.from_bytes(file.read(2))
    dd_offset = int.from_bytes(file.read(4))
    dd_length = int.from_bytes(file.read(4))
    return {
        "tag": dd_tag,
        "ref": dd_ref,
        "offset": dd_offset,
        "length": dd_length
    }

def read_dd_block(file, offset=None):
    if offset:
        file.seek(offset)
    else:
        offset = file.tell(offset)
    n_dd = int.from_bytes(file.read(2))
    next_offset = int.from_bytes(file.read(4))
    dd_list = []
    for _ in range(n_dd):
        dd = read_dd(file)
        dd_list.append(dd)
    dd_block = {
        "current_offset": offset,
        "size": n_dd,
        "next_offset": next_offset,
        "dd": dd_list
    }
    return dd_block

def read_all_dd(file):
    current_loc = file.tell()
    offset = 4
    file.seek(offset)
    ddh_list = []
    while True:
        ddh = read_dd_block(file, offset)
        ddh_list.append(ddh)
        if ddh["next_offset"] == 0:
            break
        else:
            offset = ddh["next_offset"]
    file.seek(current_loc)
    return ddh_list

dd_blocks = read_all_dd(infile)
dd_blocks[0]

# Compine all DDs into a pandas data frame, for parsing
dd_list_all = [val for item in dd_blocks for val in item["dd"]]
dd_df_all = pd.DataFrame(dd_list_all)

dd_df = dd_df_all[~((dd_df_all["tag"] == 1) & (dd_df_all["ref"] == 0))].sort_values(["ref", "tag"])

# DFTAGs:
# 20 -- DFTAG_LINKED
# 30 -- DFTAG_VERSION
# 40 -- DFTAG_COMPRESSED
# 720 -- DFTAG_NDG (numeric data group)
# 1965 -- DFTAG_VG (virtual group)
# 16445 - ???

ref = dd_df[dd_df["ref"] == 5]

# Parse the first tag in this file
dd00 = dd_blocks[0]["dd"][0]
infile.seek(dd00["offset"])
dd0_version = {
    "majorv": int.from_bytes(infile.read(4)),
    "minorv": int.from_bytes(infile.read(4)),
    "release": int.from_bytes(infile.read(4)),
    "string": infile.read(dd00["length"]-12).decode("ascii").rstrip('\x00')
}

dd01 = dd_blocks[0]["dd"][1]
infile.seek(dd01["offset"])
dd01_parsed = {}

dd02 = dd_blocks[0]["dd"][2]

dd3 = dd_blocks[3]["dd"][1]

infile.seek(dd3["offset"])
dat_3 = infile.read(dd3["length"])

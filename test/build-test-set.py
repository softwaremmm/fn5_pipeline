'''As we're currently using TB, to save significant space, 
    edit a single FASTA file to produce known variants
This uses python rather than bash, purely out of ease of writing
'''

def parse_fasta(filepath: str) -> str:
    '''Parse fasta file to get data. Ignore header

    Args:
        filepath (str): Path to FASTA file

    Returns:
        str: Single string of the data
    '''
    with open(filepath) as f:
        content = [line.strip() for line in f]
    return ''.join(content[1:])

def add_known_variants(fasta: str, variants: dict, silent: bool=False) -> str:
    '''Duplicate the contents of the given fasta string, adding provided variants

    Args:
        fasta (str): Content from a FASTA file
        variants (dict): Dict mapping pos->base
        silent (bool, optional): Suppress lines when a variant gives no effect

    Returns:
        str: FASTA content with edited bases
    '''
    f = list(fasta)
    for pos, base in variants.items():
        if f[pos] == base and not silent:
            print(f"No effect of {pos}->{base}")
        f[pos] = base
    return ''.join(f)

def write_fasta(fasta: str, path: str) -> None:
    '''Write the FASTA content with a dummy header to the given path

    Args:
        fasta (str): Content from a FASTA file
        path (str): Path of the file to write
    '''
    header = ">NC_000962.3|Mycobacterium tuberculosis H37Rv|dummy-guid\n"
    with open(path, "w") as f:
        f.write(header)
        f.write(fasta)


'''
Desired outcome:
------------------------------------
|   || 1  | 2  | 3  | 4  | 5  | 6  |
|---||-----------------------------|
| 1 || -1 | 1  | 1  | 6  | -1 | -1 |
| 2 || 1  | -1 | 0  | 7  | -1 | -1 |
| 3 || 1  | 0  | -1 | 7  | -1 | -1 |
| 4 || 6  | 7  | 7  | -1 | -1 | -1 |
| 5 || -1 | -1 | -1 | -1 | -1 | 1  |
| 6 || -1 | -1 | -1 | -1 | -1 | 1  |
------------------------------------
'''


if __name__ == "__main__":
    s1 = parse_fasta("test/sample.fasta")

    s2 = add_known_variants(s1, {
        1: "A", #From T
        12: "N" #From C
    })

    s3 = add_known_variants(s1, {
        1: "N", #From T
        12: "A" #From C
    })

    s4 = add_known_variants(s1, {
        123: "G", #From C
        1234: "C", #From G
        12345: "C", #From G
        123456: "C", #From G
        1234567: "C", #From A
        1234568: "A", #From C
    })

    #Ensure we have an orphan
    s5 = add_known_variants(s1, {
        x: "A"
        for x in range(1, 1000)
    }, silent=True)

    #For ensuring we pick up orphans which later have neighbours
    #This should give {'distances': {'5': 1}}
    s6 = add_known_variants(s5, {
        1234567: "C", #From A
    })

    #Create a QC fail by giving an all N FASTA
    s7 = "N" * len(s1)

    write_fasta(s1, "test/s1.fasta")
    write_fasta(s2, "test/s2.fasta")
    write_fasta(s3, "test/s3.fasta")
    write_fasta(s4, "test/s4.fasta")
    write_fasta(s5, "test/s5.fasta")
    write_fasta(s6, "test/s6.fasta")
    write_fasta(s7, "test/s7.fasta")


    
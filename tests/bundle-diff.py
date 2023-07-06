from pathlib import Path

def main(argv : list[str]) -> int:
    if len(argv[1:]) != 2:
        print("Usage: bundle-diff.py <bundle1> <bundle2>")
        return 1
    
    added : set[Path] = set()
    removed : set[Path] = set()

    bundle1 = Path(argv[1])
    if not bundle1.is_dir():
        print(f"Error: {bundle1} is not a directory")
        return 1
    bundle2 = Path(argv[2])
    if not bundle2.is_dir():
        print(f"Error: {bundle2} is not a directory")
        return 1
    
    bundle1 = bundle1.absolute()
    bundle2 = bundle2.absolute()
    
    for p in bundle1.glob("**/*"):
        subpath = p.absolute().relative_to(bundle1)
        #print(subpath)
        if not set(subpath.parents).isdisjoint(removed):
            continue
        path_in_bundle2 = bundle2 / subpath
        #print(path_in_bundle2)
        if not path_in_bundle2.exists():
            removed.add(subpath)

    for p in bundle2.glob("**/*"):
        subpath = p.absolute().relative_to(bundle2)
        if not set(subpath.parents).isdisjoint(added):
            continue
        path_in_bundle1 = bundle1 / subpath
        if not path_in_bundle1.exists():
            added.add(subpath)
    
    for p in sorted(added):
        print(f"+ {p}")

    for p in sorted(removed):
        print(f"- {p}")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main(sys.argv))
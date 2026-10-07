import sys

def main():
    if len(sys.argv) < 2:
        print("bt <run|rerun|report> ...")
        return 1
    cmd = sys.argv[1]
    if cmd == "run" and len(sys.argv) >= 3:
        print(f"[bt] run {sys.argv[2]} (placeholder)")
        return 0
    if cmd == "rerun" and len(sys.argv) >= 3:
        print(f"[bt] rerun {sys.argv[2]} (placeholder)")
        return 0
    if cmd == "report" and len(sys.argv) >= 3:
        print(f"[bt] report {sys.argv[2]} (placeholder)")
        return 0
    print("comando no reconocido")
    return 1


if __name__ == "__main__":
    sys.exit(main())

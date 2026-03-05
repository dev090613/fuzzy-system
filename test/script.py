from pathlib import Path

current_dir = Path.cwd()
current_file = Path(__file__).name
print(f"Current Directory: {current_dir}")
print(f"Current File: {current_file}\n")

for filepath in current_dir.iterdir():
    # print(filepath.name)
    if filepath.name == current_file:
        continue

    print(f"- {filepath.name}")
    if filepath.is_file:
        # print(filepath.name)
        content = filepath.read_text(encoding="utf-8")
        print(f"     Content: {content}")

    # read.text

print("\nProgram excuted")

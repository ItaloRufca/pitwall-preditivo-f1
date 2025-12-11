import json

nb_path = "/Users/italorufca/Desktop/Italo/Projetos/pitwall-preditivo-f1/notebooks/feature_engineering_and_split.ipynb"

with open(nb_path, "r", encoding="utf-8") as f:
    nb = json.load(f)

found = False
for cell in nb["cells"]:
    if cell["cell_type"] == "code":
        source = cell["source"]
        # Check for the specific line and if the fix is already there
        has_buggy_line = any("train_data = df_final[df_final['year'] <= 2024]" in line for line in source)
        has_fix = any("astype(int)" in line for line in source)
        
        if has_buggy_line and not has_fix:
            new_source = []
            for line in source:
                if "train_data = df_final[df_final['year'] <= 2024]" in line:
                    # Inject the fix
                    new_source.append("    # Convert 'year' column to integer to allow numerical comparison\n")
                    new_source.append("    df_final['year'] = df_final['year'].astype(int)\n")
                    new_source.append("\n")
                new_source.append(line)
            cell["source"] = new_source
            found = True
            break

if found:
    with open(nb_path, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=4)
    print("Notebook fixed successfully.")
else:
    print("Buggy line not found or already fixed.")


import os
import glob

def fix_file(filepath, replacements):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        new_content = content
        for old, new in replacements.items():
            new_content = new_content.replace(old, new)
            
        if new_content != content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Updated: {filepath}")
        else:
            print(f"No changes needed: {filepath}")
    except Exception as e:
        print(f"Error fixing {filepath}: {e}")

def main():
    # 1. Update eda_gold.ipynb
    eda_path = os.path.join("notebooks", "eda_gold.ipynb")
    replacements = {
        "gold_race_wt": "gold_race_widetable",
        "gold_practice_wt": "gold_practice_widetable"
    }
    if os.path.exists(eda_path):
        fix_file(eda_path, replacements)
    else:
        print(f"Not found: {eda_path}")

    # 2. Update Model Feature List (json model file if text based? No, it's binary/json mix often, skip)
    
    # 3. Cleanup unused notebooks
    # We remove notebooks that are superseded by python scripts in scripts/
    # to prevent confusion (since we can't easily maintain .ipynb files via tool).
    to_remove = [
        "notebooks/feature_engineering_fixed.ipynb",
        "notebooks/feature_engineering_and_split.ipynb", 
        "notebooks/train_model.ipynb"
    ]
    
    for p in to_remove:
        if os.path.exists(p):
            try:
                os.remove(p)
                print(f"Removed unused notebook: {p}")
            except Exception as e:
                print(f"Error removing {p}: {e}")

if __name__ == "__main__":
    main()

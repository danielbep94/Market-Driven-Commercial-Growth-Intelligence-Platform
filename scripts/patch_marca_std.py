#!/usr/bin/env python3
"""
patch_marca_std.py — C.3
Adds MARCA_STD CASE WHEN to 5 remaining layouts:
  SELL_IN, IBP, MKT_ON, MKT_OFF, WASTE
Each layout already has MARCA in SELECT; we inject MARCA_STD right after.
"""

import re, os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MARCA_STD_BLOCK = """CASE
                WHEN TRIM(UPPER(MARCA)) IN ('ACTIVIA') THEN 'ACTIVIA'
                WHEN TRIM(UPPER(MARCA)) IN ('AGUAS FRESCAS', 'BFT AGUAS FRESCAS', 'BONAFONT_AGUASFRESCAS', 'BONAFONT AGUA FRESCAS') THEN 'AGUAS FRESCAS'
                WHEN TRIM(UPPER(MARCA)) IN ('BADOIT') THEN 'BADOIT'
                WHEN TRIM(UPPER(MARCA)) IN ('BENEGASTRO', 'BENEG') THEN 'BENEGASTRO'
                WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT', 'BONAFONT_NATURAL', 'AZUL BONAFONT') THEN 'BONAFONT'
                WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT JUGO') THEN 'BONAFONT JUGO'
                WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT KIDS', 'BONAFONT_KIDS', 'BONAFONT KIDS NATURAL') THEN 'BONAFONT KIDS'
                WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT MINERAL', 'MINERALIZADA') THEN 'BONAFONT MINERAL'
                WHEN TRIM(UPPER(MARCA)) IN ('BONAFONT SER', 'SER PURA') THEN 'BONAFONT SER'
                WHEN TRIM(UPPER(MARCA)) IN ('TE BONAFONT', 'TÉ BONAFONT', 'BONAFONT TE', 'BONAFONT_TE', 'BONAFONT TÉ') THEN 'BONAFONT TE'
                WHEN TRIM(UPPER(MARCA)) IN ('DANETTE') THEN 'DANETTE'
                WHEN TRIM(UPPER(MARCA)) IN ('DANISSIMO') THEN 'DANISSIMO'
                WHEN TRIM(UPPER(MARCA)) IN ('DANMIX', 'DAN MIX', 'DANMMIX') THEN 'DANMIX'
                WHEN TRIM(UPPER(MARCA)) IN ('DANONE', 'DANONE YOGHURT', 'DANONE CREME', 'DANONE FREE', 'DANONE GRIEGO', 'DANONE FS', 'DAIRY') THEN 'DANONE'
                WHEN TRIM(UPPER(MARCA)) IN ('DANONINO', 'DANONINOLIQUIDO') THEN 'DANONINO'
                WHEN TRIM(UPPER(MARCA)) IN ('DANUP', 'DAN UP', 'DAN\\'UP') THEN 'DANUP'
                WHEN TRIM(UPPER(MARCA)) IN ('DANY', 'DANY DANETTE') THEN 'DANY'
                WHEN TRIM(UPPER(MARCA)) IN ('DELIGHT') THEN 'DELIGHT'
                WHEN TRIM(UPPER(MARCA)) IN ('EVIAN') THEN 'EVIAN'
                WHEN TRIM(UPPER(MARCA)) IN ('HERSHEYS', 'HERSHEY\\'S', 'DANONE HERSHEYS') THEN 'HERSHEYS'
                WHEN TRIM(UPPER(MARCA)) IN ('INFINIT WATER', 'INFINIT', 'INFINITY', 'WATER INFINIT') THEN 'INFINIT'
                WHEN TRIM(UPPER(MARCA)) IN ('JUIZZY') THEN 'JUIZZY'
                WHEN TRIM(UPPER(MARCA)) IN ('LEVITE', 'BONAFONT_LEVITE', 'BONAFONT LEVITE') THEN 'LEVITE'
                WHEN TRIM(UPPER(MARCA)) IN ('LICUAMIX') THEN 'LICUAMIX'
                WHEN TRIM(UPPER(MARCA)) IN ('OCEAN SPRAY', 'OCEAN') THEN 'OCEAN SPRAY'
                WHEN TRIM(UPPER(MARCA)) IN ('OIKOS', 'OIKOS UHT') THEN 'OIKOS'
                WHEN TRIM(UPPER(MARCA)) IN ('PUREZA AGA', 'AGA', 'AGA 20 LTS', 'AQUAPURA', 'AGUA NATURAL') THEN 'PUREZA AGA'
                WHEN TRIM(UPPER(MARCA)) IN ('SILK', 'SILK ORIG 946ML', 'SILKCHOCO190ML') THEN 'SILK'
                WHEN TRIM(UPPER(MARCA)) IN ('SO DELICIOUS') THEN 'SO DELICIOUS'
                WHEN TRIM(UPPER(MARCA)) IN ('STOK COLD BREW', 'STOK') THEN 'STOK'
                WHEN TRIM(UPPER(MARCA)) IN ('VITALINEA') THEN 'VITALINEA'
                WHEN TRIM(UPPER(MARCA)) IN ('YOPRO', 'YO PRO') THEN 'YOPRO'
                WHEN TRIM(UPPER(MARCA)) IN ('COCA COLA', 'COCA-COLA', 'COCACOLA', 'THE COCA COLA EXPORT', 'COCA COLA FEMSA') THEN 'COCA COLA'
                WHEN TRIM(UPPER(MARCA)) IN ('PEPSI', 'PEPSI COLA MEXICANA', 'PEPSICOLA MEXICANA') THEN 'PEPSI'
                WHEN TRIM(UPPER(MARCA)) IN ('LALA', 'GPO INDUSTRIAL LALA', 'LALA_BIO4', 'LALA_CHIQUITIN', 'LALA_CREMA', 'LALA_GRIEGO', 'LALA_LECHE', 'LALA_LECHE_100', 'LALA_MAESTROS', 'LALA_NATURALES', 'LALA_PLENIA', 'LALA_PROBIOC', 'LALA_QUESOS', 'LALA_YOGURT', 'LALA_YOGURT_100', 'LALA_YOMI') THEN 'LALA'
                WHEN TRIM(UPPER(MARCA)) IN ('ALPURA', 'ALPURA_GRIEGO', 'ALPURA_LECHES', 'ALPURA_TOYS', 'ALPURA_YOGURT', 'ALPURA_YOGURT_COLAGENO', 'ALPURA_YOGURT_DESLACTOSADO') THEN 'ALPURA'
                WHEN TRIM(UPPER(MARCA)) IN ('YOPLAIT', 'YOPLAIT_DOBLECERO', 'YOPLAIT_GRIEGO', 'YOPLAIT_GRIE_GO', 'YOPLAIT_KIDS', 'YOPLAIT_SKYR') THEN 'YOPLAIT'
                WHEN TRIM(UPPER(MARCA)) IN ('JUMEX', 'JUMEXITO', 'JUMEX_AMI', 'JUMEX_BIDA', 'JUMEX_CERO', 'JUMEX_FRESCO', 'JUMEX_FRESH', 'JUMEX_FRUTZZO', 'JUMEX_HYDROLIT', 'JUMEX_MIA', 'JUMEX_SPORT', 'JUMEX_UNICO', 'JUMEX_XODA', 'JUMEX_XOT', 'JUGOS DEL VALLE') THEN 'JUMEX'
                WHEN TRIM(UPPER(MARCA)) IN ('SANTA CLARA', 'SANTA_CLARA', 'SOC COOP TRAB PASCUA') THEN 'SANTA CLARA'
                WHEN TRIM(UPPER(MARCA)) IN ('CIEL', 'CIEL_EXPRIM', 'CIEL_MINERAL') THEN 'CIEL'
                WHEN TRIM(UPPER(MARCA)) IN ('PENAFIEL', 'PEÑAFIEL') THEN 'PENAFIEL'
                WHEN TRIM(UPPER(MARCA)) IN ('SEVEN UP', 'SEVENUP', 'SEVEN UP CHI') THEN 'SEVEN UP'
                WHEN TRIM(UPPER(MARCA)) IN ('LONCHERA', 'PALAS', 'TABLAS', 'TAZAS', 'VASOS', 'TOALLAS', 'TUPPER', 'UTENSILIOS', 'CERAMICA', 'VAJILLA', 'LAPICERA', 'COSMETIQUERA', 'PLAYERA MUNDIAL', 'PORTAVASOS', 'LIBRO NAVIDEÑO', 'REFRACTARIOS', 'BOTANERO', 'DISPENSERS', 'BEBEDERO', 'BOMBA', 'JARRA', 'TERMO', 'ALCANCIA', 'METALICO', 'ESPECIERO', 'POUCH', 'BOWLS') THEN '_MERCHANDISE'
                WHEN TRIM(UPPER(MARCA)) IN ('0', 'MULTI', 'DAIRY') THEN '_UNKNOWN'
                WHEN TRIM(UPPER(MARCA)) IN ('MULTIBRAND', 'MULTIBRAND DAIRY', 'MULTIBRAND DANONE', 'MULTIBRAND INDULGENCE', 'MULTIBRAND INNOS', 'MULTIBRAND KIDS', 'MULTIBRAND WATERS', 'MULTIMARCA', 'INSTITUTO DANONE', 'DNP', 'DANONE FS') THEN '_MULTIBRAND'
                ELSE TRIM(UPPER(MARCA))
            END AS MARCA_STD"""

# Each entry: (file_path, search_pattern, replacement_inject_after)
# Strategy: find "MARCA," or "MARCA\n" in the SELECT block and inject MARCA_STD after it

LAYOUTS = {
    "SELL_IN":  "SEMANTIC_LAYOUTS/SELL_IN/SELL_IN_DICT.txt",
    "IBP":      "SEMANTIC_LAYOUTS/IBP/IBP_DANONE.txt",
    "MKT_ON":   "SEMANTIC_LAYOUTS/MKT_ON/ON.txt",
    "MKT_OFF":  "SEMANTIC_LAYOUTS/MKT_OFF/OFF.txt",
    "WASTE":    "SEMANTIC_LAYOUTS/WASTE/DATA_WASTE.txt",
}

def inject_marca_std(content, layout_name):
    """Inject MARCA_STD after first occurrence of MARCA in SELECT list."""
    # Find "AS MARCA," or just "MARCA," in select positions
    # We'll look for the pattern and insert after
    pattern = r'([ \t]+(?:AS\s+)?MARCA,?\s*\n)'

    # Only inject once (first match)
    injected = False
    lines = content.split("\n")
    out = []
    marca_seen = False
    for i, line in enumerate(lines):
        out.append(line)
        # Look for MARCA line in a SELECT block (not in metadata/grain_cols lists)
        stripped = line.strip()
        if not marca_seen and re.match(r'.*\bMARCA\b,?\s*$', stripped) and \
           not any(k in stripped for k in ['"MARCA"', "'MARCA'", "#", "grain", "business", "metric", "note"]):
            # Insert MARCA_STD after this line with same indentation
            indent = len(line) - len(line.lstrip())
            marca_std_lines = MARCA_STD_BLOCK.strip().split("\n")
            # Adjust indentation relative to the MARCA line
            adjusted = []
            base_indent = " " * indent
            for ml in marca_std_lines:
                # The block already has 16 spaces of indent; replace with layout's indent
                adjusted.append(base_indent + ml.lstrip() if ml.strip() else ml)
            out.extend(adjusted)
            out.append("")  # trailing newline after the block
            marca_seen = True
            injected = True
    if not injected:
        print(f"  ⚠️  Could not find MARCA line to inject after in {layout_name}")
    return "\n".join(out)

def add_marca_std_to_grain_cols(content):
    """Add 'MARCA_STD' to grain_cols and business_keys lists in metadata."""
    # Add after "MARCA" in grain_cols
    content = re.sub(
        r'("grain_cols":\s*\[[^\]]*?"MARCA")',
        lambda m: m.group(0).rstrip() + ',\n            "MARCA_STD"',
        content,
        count=1
    )
    # Add after "MARCA" in business_keys (if present)
    content = re.sub(
        r'("business_keys":\s*\[[^\]]*?"MARCA")',
        lambda m: m.group(0).rstrip() + ',\n            "MARCA_STD"',
        content,
        count=1
    )
    return content

for name, rel_path in LAYOUTS.items():
    path = os.path.join(REPO, rel_path)
    with open(path, encoding="utf-8") as f:
        content = f.read()

    # Skip if already patched
    if "MARCA_STD" in content:
        print(f"  ⏭️  {name}: already has MARCA_STD, skipping")
        continue

    new_content = inject_marca_std(content, name)
    new_content = add_marca_std_to_grain_cols(new_content)

    with open(path, "w", encoding="utf-8") as f:
        f.write(new_content)
    print(f"  ✓ {name}: MARCA_STD injected")

print("\nDone. Rebuild registry to verify.")

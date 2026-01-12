from __future__ import annotations

import json
import re
import shutil
import subprocess
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CirceField:
    json_property: str
    java_type: str
    java_field: str


_CLASS_RE = re.compile(
    r"^\s*public\s+(?:abstract\s+)?class\s+"
    r"(?P<name>[A-Za-z0-9_]+)\b"
    r"(?:\s+extends\s+(?P<base>[A-Za-z0-9_]+)\b)?"
)
_JSON_PROPERTY_RE = re.compile(r'^\s*@JsonProperty\("(?P<prop>[^"]+)"\)\s*$')
_FIELD_RE = re.compile(
    r"^\s*public\s+(?P<type>[A-Za-z0-9_<>,\[\]]+)\s+(?P<name>[A-Za-z0-9_]+)\s*(?:=\s*[^;]+)?;\s*$"
)


def find_circe_jar(*, rscript_path: str | None = None) -> Path:
    """
    Locate the Circe Java JAR shipped with CirceR.

    Used for extracting the Circe @JsonProperty inventory without compiling Java.
    """
    rscript_exe = rscript_path or shutil.which("Rscript")
    if not rscript_exe:
        raise RuntimeError("Rscript not found; cannot locate CirceR's Circe JAR.")

    cmd = [
        rscript_exe,
        "--vanilla",
        "-e",
        r'cat(Sys.glob(file.path(system.file(package="CirceR"), "java", "circe-*.jar")))',
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            "Failed to locate CirceR Circe JAR via Rscript.\n"
            f"stderr:\n{(result.stderr or '').strip()}\n"
            f"stdout:\n{(result.stdout or '').strip()}\n"
        )

    jar_path = (result.stdout or "").strip()
    if not jar_path:
        raise FileNotFoundError("CirceR Circe JAR not found (Sys.glob returned empty).")

    path = Path(jar_path)
    if not path.exists():
        raise FileNotFoundError(f"CirceR Circe JAR not found at: {path}")
    return path


_JAVAP_CLASS_RE = re.compile(
    r"^\s*public\s+(?:abstract\s+)?class\s+"
    r"(?P<name>[A-Za-z0-9_.$]+)\b"
    r"(?:\s+extends\s+(?P<base>[A-Za-z0-9_.$]+)\b)?"
)
_JAVAP_FIELD_DECL_RE = re.compile(r"^\s*public\s+(?P<type>[^();]+?)\s+(?P<name>[A-Za-z0-9_]+);?\s*$")
_JAVAP_DESCRIPTOR_RE = re.compile(r"^\s*descriptor:\s+(?P<desc>\S+)\s*$")
_JAVAP_SIGNATURE_RE = re.compile(r"^\s*Signature:\s+(?P<sig>\S+)\s*$")
_JAVAP_JSONPROP_VALUE_RE = re.compile(r'^\s*value="(?P<value>[^"]+)"\s*$')


def _simplify_descriptor(descriptor: str, signature: str | None) -> str:
    # Prefer signature when present for generic collections (List<...>).
    if signature and signature.startswith("Ljava/util/List<") and signature.endswith(">;"):
        # Example: Ljava/util/List<Lorg/ohdsi/circe/cohortdefinition/InclusionRule;>;
        inner = signature.removeprefix("Ljava/util/List<").removesuffix(">;")
        if inner.startswith("L") and inner.endswith(";"):
            inner_name = inner[1:-1].split("/")[-1]
            return f"List<{inner_name}>"
        return "List<Unknown>"

    if descriptor == "Ljava/lang/String;":
        return "String"
    if descriptor in {"I", "J", "D", "F", "Z"}:
        return {"I": "int", "J": "long", "D": "double", "F": "float", "Z": "boolean"}[descriptor]

    if descriptor.startswith("[") and descriptor.endswith(";"):
        # Arrays of objects.
        inner = descriptor[2:-1] if descriptor.startswith("[L") else descriptor[1:]
        inner_name = inner.split("/")[-1]
        return f"{inner_name}[]"

    if descriptor.startswith("L") and descriptor.endswith(";"):
        name = descriptor[1:-1].split("/")[-1]
        return name

    # Fallback to raw descriptor.
    return descriptor


def extract_circe_field_inventory_from_jar(
    circe_jar: Path, *, include_inherited: bool = True
) -> dict[str, list[CirceField]]:
    circe_jar = circe_jar.resolve()
    if not circe_jar.exists():
        raise FileNotFoundError(f"Circe jar not found: {circe_jar}")

    javap_exe = shutil.which("javap")
    if not javap_exe:
        raise RuntimeError("javap not found; install a JDK to extract Circe inventory from jar.")

    class_names: list[str] = []
    with zipfile.ZipFile(circe_jar) as zf:
        for name in zf.namelist():
            if not name.endswith(".class"):
                continue
            if "$" in name:
                continue
            if not name.startswith("org/ohdsi/circe/cohortdefinition/"):
                continue
            class_names.append(name[:-6].replace("/", "."))
    class_names.sort()

    inventory: dict[str, list[CirceField]] = {}
    bases: dict[str, str] = {}

    def parse_one_class(lines: list[str]) -> None:
        if not lines:
            return

        class_simple: str | None = None
        base_simple: str | None = None
        for line in lines:
            m = _JAVAP_CLASS_RE.match(line)
            if m:
                class_simple = m.group("name").split(".")[-1]
                base = m.group("base")
                if base:
                    base_simple = base.split(".")[-1]
                break
        if not class_simple:
            return
        if base_simple:
            bases[class_simple] = base_simple

        fields: list[CirceField] = []
        current_field: str | None = None
        current_descriptor: str | None = None
        current_signature: str | None = None
        in_jsonprop: bool = False
        field_finalized: bool = False

        for line in lines:
            decl = _JAVAP_FIELD_DECL_RE.match(line)
            if decl and "(" not in line:
                current_field = decl.group("name")
                current_descriptor = None
                current_signature = None
                in_jsonprop = False
                field_finalized = False
                continue

            if current_field is None or field_finalized:
                continue

            desc_m = _JAVAP_DESCRIPTOR_RE.match(line)
            if desc_m:
                current_descriptor = desc_m.group("desc")
                continue

            sig_m = _JAVAP_SIGNATURE_RE.match(line)
            if sig_m:
                current_signature = sig_m.group("sig")
                continue

            # JsonProperty annotation block: capture `value="..."`.
            if "com.fasterxml.jackson.annotation.JsonProperty" in line:
                in_jsonprop = True
                continue
            if in_jsonprop:
                val_m = _JAVAP_JSONPROP_VALUE_RE.match(line)
                if val_m and current_descriptor:
                    fields.append(
                        CirceField(
                            json_property=val_m.group("value"),
                            java_type=_simplify_descriptor(current_descriptor, current_signature),
                            java_field=current_field,
                        )
                    )
                    field_finalized = True
                    in_jsonprop = False

        if fields:
            fields.sort(key=lambda f: f.json_property)
            inventory[class_simple] = fields

    def iter_batches(items: list[str], batch_size: int) -> Iterable[list[str]]:
        for i in range(0, len(items), batch_size):
            yield items[i : i + batch_size]

    # `javap` startup is expensive; batch classes into fewer processes.
    for batch in iter_batches(class_names, 64):
        result = subprocess.run(
            [javap_exe, "-classpath", str(circe_jar), "-v", *batch],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            continue
        lines = (result.stdout or "").splitlines()
        if not lines:
            continue

        # Split combined output by "Classfile ..." header.
        starts = [i for i, line in enumerate(lines) if line.startswith("Classfile ")]
        if not starts:
            continue
        starts.append(len(lines))
        for s, e in zip(starts, starts[1:]):
            parse_one_class(lines[s:e])

    if not include_inherited:
        return inventory

    def merge_fields(name: str, *, seen: set[str]) -> list[CirceField]:
        if name in seen:
            return list(inventory.get(name, []))
        seen.add(name)
        combined: list[CirceField] = list(inventory.get(name, []))
        base = bases.get(name)
        if base and base in inventory:
            combined.extend(merge_fields(base, seen=seen))
        out: list[CirceField] = []
        seen_props: set[str] = set()
        for f in combined:
            if f.json_property in seen_props:
                continue
            seen_props.add(f.json_property)
            out.append(f)
        return out

    merged: dict[str, list[CirceField]] = {}
    for name in inventory:
        merged[name] = merge_fields(name, seen=set())
    return merged


def extract_circe_field_inventory(
    cohortdefinition_dir: Path, *, include_inherited: bool = True
) -> dict[str, list[CirceField]]:
    """
    Extract a machine-readable list of Circe cohort-definition JSON properties from the Circe Java source.

    This is intentionally lightweight (regex-based) and avoids any Java compilation.
    """
    cohortdefinition_dir = cohortdefinition_dir.resolve()
    if cohortdefinition_dir.is_file() and cohortdefinition_dir.suffix == ".jar":
        return extract_circe_field_inventory_from_jar(cohortdefinition_dir, include_inherited=include_inherited)
    if not cohortdefinition_dir.exists():
        raise FileNotFoundError(f"Circe cohortdefinition directory not found: {cohortdefinition_dir}")

    inventory: dict[str, list[CirceField]] = {}
    bases: dict[str, str] = {}
    for java_path in sorted(cohortdefinition_dir.glob("*.java")):
        text = java_path.read_text(encoding="utf-8", errors="replace").splitlines()
        class_name: str | None = None
        base_name: str | None = None
        for line in text:
            match = _CLASS_RE.match(line)
            if match:
                class_name = match.group("name")
                base_name = match.group("base")
                break
        if not class_name:
            continue
        if base_name:
            bases[class_name] = base_name

        fields: list[CirceField] = []
        pending_prop: str | None = None
        for line in text:
            prop_match = _JSON_PROPERTY_RE.match(line)
            if prop_match:
                pending_prop = prop_match.group("prop")
                continue
            if pending_prop is None:
                continue
            field_match = _FIELD_RE.match(line)
            if not field_match:
                continue
            fields.append(
                CirceField(
                    json_property=pending_prop,
                    java_type=field_match.group("type"),
                    java_field=field_match.group("name"),
                )
            )
            pending_prop = None

        if fields:
            inventory[class_name] = fields

    if not include_inherited:
        return inventory

    def merge_fields(name: str, *, seen: set[str]) -> list[CirceField]:
        if name in seen:
            return list(inventory.get(name, []))
        seen.add(name)
        combined: list[CirceField] = list(inventory.get(name, []))
        base = bases.get(name)
        if base and base in inventory:
            combined.extend(merge_fields(base, seen=seen))
        # De-duplicate by json_property, keeping the first (child overrides base).
        out: list[CirceField] = []
        seen_props: set[str] = set()
        for f in combined:
            if f.json_property in seen_props:
                continue
            seen_props.add(f.json_property)
            out.append(f)
        return out

    merged: dict[str, list[CirceField]] = {}
    for name in inventory:
        merged[name] = merge_fields(name, seen=set())
    return merged


def circe_inventory_to_jsonable(inventory: dict[str, list[CirceField]]) -> dict[str, list[dict[str, str]]]:
    out: dict[str, list[dict[str, str]]] = {}
    for class_name, fields in inventory.items():
        out[class_name] = [
            {
                "json_property": field.json_property,
                "java_type": field.java_type,
                "java_field": field.java_field,
            }
            for field in fields
        ]
    return out


def write_circe_field_inventory(path: Path, inventory: dict[str, list[CirceField]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = circe_inventory_to_jsonable(inventory)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

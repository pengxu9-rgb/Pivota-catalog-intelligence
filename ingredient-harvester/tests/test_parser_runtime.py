from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path


def test_parser_runtime_loads_vendor_parser_without_services_dir(tmp_path: Path) -> None:
    source_app_dir = Path(__file__).resolve().parents[1] / "app"
    temp_app_dir = tmp_path / "app"
    temp_app_dir.mkdir()

    shutil.copyfile(source_app_dir / "parser_runtime.py", temp_app_dir / "parser_runtime.py")
    shutil.copyfile(source_app_dir / "ingredient_parser_vendor.py", temp_app_dir / "ingredient_parser_vendor.py")

    spec = importlib.util.spec_from_file_location("isolated_parser_runtime", temp_app_dir / "parser_runtime.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.parser_ready() is True
    snapshot = module.build_parser_snapshot("Ingredients: Water, Glycerin")
    assert snapshot["parse_status"] == "OK"
    assert snapshot["inci_list_json"][0]["standard_name"] == "Aqua"


def test_parser_runtime_strips_full_ingredients_list_prefix(tmp_path: Path) -> None:
    source_app_dir = Path(__file__).resolve().parents[1] / "app"
    temp_app_dir = tmp_path / "app"
    temp_app_dir.mkdir()

    shutil.copyfile(source_app_dir / "parser_runtime.py", temp_app_dir / "parser_runtime.py")
    shutil.copyfile(source_app_dir / "ingredient_parser_vendor.py", temp_app_dir / "ingredient_parser_vendor.py")

    spec = importlib.util.spec_from_file_location("isolated_parser_runtime_full_list", temp_app_dir / "parser_runtime.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    snapshot = module.build_parser_snapshot("Full Ingredients List: Aqua/Water/Eau, Glycerin")
    assert snapshot["parse_status"] == "OK"
    assert snapshot["cleaned_text"].startswith("Aqua/Water/Eau, Glycerin")
    assert snapshot["inci_list_json"][0]["standard_name"] == "Aqua"

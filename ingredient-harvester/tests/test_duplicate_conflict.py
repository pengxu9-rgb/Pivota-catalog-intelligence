from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.main import _duplicate_conflict
from app.models import Base, CandidateRow, ImportBatch


def _session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)()


def test_duplicate_conflict_ignores_historical_unapproved_rows() -> None:
    db = _session()
    try:
        db.add_all(
            [
                ImportBatch(import_id="imp_old", filename="old.csv"),
                ImportBatch(import_id="imp_new", filename="new.csv"),
            ]
        )
        db.add_all(
            [
                CandidateRow(
                    row_id="old_row",
                    import_id="imp_old",
                    row_index=0,
                    product_name="Barrier Booster",
                    sku_key="sku_1",
                    candidate_id="sku_1",
                    inci_list="CosDNA noise; Aqua",
                    review_status="UNREVIEWED",
                    audit_status="REVIEW",
                ),
                CandidateRow(
                    row_id="new_row",
                    import_id="imp_new",
                    row_index=0,
                    product_name="Barrier Booster",
                    sku_key="sku_1",
                    candidate_id="sku_1",
                    inci_list="Aqua; Glycerin",
                    review_status="UNREVIEWED",
                    audit_status="PASS",
                ),
            ]
        )
        db.commit()

        row = db.get(CandidateRow, "new_row")
        assert row is not None
        assert _duplicate_conflict(db, row) is None
    finally:
        db.close()


def test_duplicate_conflict_keeps_same_import_conflicts_blocking() -> None:
    db = _session()
    try:
        db.add(ImportBatch(import_id="imp_same", filename="same.csv"))
        db.add_all(
            [
                CandidateRow(
                    row_id="row_a",
                    import_id="imp_same",
                    row_index=0,
                    product_name="Barrier Booster",
                    sku_key="sku_1",
                    candidate_id="sku_1",
                    inci_list="Aqua; Glycerin",
                    review_status="UNREVIEWED",
                    audit_status="PASS",
                ),
                CandidateRow(
                    row_id="row_b",
                    import_id="imp_same",
                    row_index=1,
                    product_name="Barrier Booster",
                    sku_key="sku_1",
                    candidate_id="sku_1",
                    inci_list="Aqua; CosDNA noise",
                    review_status="UNREVIEWED",
                    audit_status="REVIEW",
                ),
            ]
        )
        db.commit()

        row = db.get(CandidateRow, "row_a")
        assert row is not None
        conflict = _duplicate_conflict(db, row)
        assert conflict is not None
        assert conflict["conflicting_row_id"] == "row_b"
        assert conflict["conflicting_import_id"] == "imp_same"
    finally:
        db.close()


def test_duplicate_conflict_keeps_historical_approved_rows_blocking() -> None:
    db = _session()
    try:
        db.add_all(
            [
                ImportBatch(import_id="imp_old", filename="old.csv"),
                ImportBatch(import_id="imp_new", filename="new.csv"),
            ]
        )
        db.add_all(
            [
                CandidateRow(
                    row_id="approved_old",
                    import_id="imp_old",
                    row_index=0,
                    product_name="Barrier Booster",
                    sku_key="sku_1",
                    candidate_id="sku_1",
                    inci_list="Aqua; Legacy",
                    review_status="APPROVED",
                    audit_status="PASS",
                ),
                CandidateRow(
                    row_id="new_row",
                    import_id="imp_new",
                    row_index=0,
                    product_name="Barrier Booster",
                    sku_key="sku_1",
                    candidate_id="sku_1",
                    inci_list="Aqua; Glycerin",
                    review_status="UNREVIEWED",
                    audit_status="PASS",
                ),
            ]
        )
        db.commit()

        row = db.get(CandidateRow, "new_row")
        assert row is not None
        conflict = _duplicate_conflict(db, row)
        assert conflict is not None
        assert conflict["conflicting_row_id"] == "approved_old"
        assert conflict["conflicting_review_status"] == "APPROVED"
    finally:
        db.close()


def test_duplicate_conflict_ignores_historical_generic_official_rows_when_current_row_is_variant_specific_official() -> None:
    db = _session()
    try:
        db.add_all(
            [
                ImportBatch(import_id="imp_old", filename="old.csv"),
                ImportBatch(import_id="imp_new", filename="new.csv"),
            ]
        )
        db.add_all(
            [
                CandidateRow(
                    row_id="approved_old",
                    import_id="imp_old",
                    row_index=0,
                    product_name="On-the-Glow Blush - Mauve",
                    sku_key="extseed:eps_1:42457583812704",
                    candidate_id="extseed:eps_1:42457583812704",
                    source_ref="https://www.pixibeauty.com/products/on-the-glow-blush",
                    source_type="Official",
                    inci_list="CheekTone; Legacy",
                    review_status="APPROVED",
                    audit_status="PASS",
                ),
                CandidateRow(
                    row_id="new_row",
                    import_id="imp_new",
                    row_index=0,
                    product_name="On-the-Glow Blush - Mauve",
                    sku_key="extseed:eps_1:42457583812704",
                    candidate_id="extseed:eps_1:42457583812704",
                    source_ref="https://www.pixibeauty.com/products/on-the-glow-blush?variant=42457583812704",
                    source_type="Official",
                    inci_list="Diisostearyl Malate; Caprylic/Capric Triglyceride",
                    review_status="UNREVIEWED",
                    audit_status="PASS",
                ),
            ]
        )
        db.commit()

        row = db.get(CandidateRow, "new_row")
        assert row is not None
        assert _duplicate_conflict(db, row) is None
    finally:
        db.close()

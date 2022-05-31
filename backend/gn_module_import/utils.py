import os
from io import StringIO
import csv
import json

from sqlalchemy import func
from chardet.universaldetector import UniversalDetector
from sqlalchemy.sql.expression import select, insert, literal
import sqlalchemy as sa
import pandas as pd
import numpy as np
from sqlalchemy.dialects.postgresql import insert as pg_insert
from werkzeug.exceptions import BadRequest
from geonature.utils.env import db

from gn_module_import import MODULE_CODE
from gn_module_import.models import (
    BibFields,
    ImportSyntheseData,
)

from geonature.core.gn_commons.models import TModules
from geonature.core.gn_synthese.models import Synthese, corAreaSynthese
from ref_geo.models import LAreas


def get_file_size(f):
    current_position = f.tell()
    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(current_position)
    return size


def detect_encoding(f):
    position = f.tell()
    f.seek(0)
    detector = UniversalDetector()
    for row in f:
        detector.feed(row)
        if detector.done:
            break
    detector.close()
    f.seek(position)
    return detector.result["encoding"]


def detect_separator(f, encoding):
    position = f.tell()
    f.seek(0)
    sample = f.readline().decode(encoding)
    if sample == '\n':  # files that do not start with column names
        raise BadRequest("File must start with columns")
    dialect = csv.Sniffer().sniff(sample)
    f.seek(position)
    return dialect.delimiter


def get_valid_bbox(imprt):
    stmt = db.session.query(
        func.ST_AsGeojson(func.ST_Extent(ImportSyntheseData.the_geom_4326))
    ).filter(
        ImportSyntheseData.imprt == imprt,
        ImportSyntheseData.valid == True,
    )
    (valid_bbox,) = db.session.execute(stmt).fetchone()
    if valid_bbox:
        return json.loads(valid_bbox)


def insert_import_data_in_database(imprt):
    columns = imprt.columns
    extra_columns = set(columns) - set(imprt.fieldmapping.values())
    csvfile = StringIO(imprt.source_file.decode(imprt.encoding))
    csvreader = csv.DictReader(
        csvfile, fieldnames=columns, delimiter=imprt.separator
    )
    header = next(csvreader, None)  # skip header
    for key, value in header.items():  # FIXME
        assert key == value
    fields = BibFields.query.filter_by(autogenerated=False).all()
    fieldmapping = {
        field.source_column: imprt.fieldmapping[field.name_field]
        for field in fields
        if (
            field.name_field in imprt.fieldmapping
            and imprt.fieldmapping[field.name_field] in columns
        )
    }
    obs = []
    line_no = 0
    for row in csvreader:
        line_no += 1
        assert list(row.keys()) == columns
        o = {
            "id_import": imprt.id_import,
            "line_no": line_no,
        }
        o.update(
            {
                dest_field: row[source_field]
                for dest_field, source_field in fieldmapping.items()
            }
        )
        o.update(
            {
                "extra_fields": {col: row[col] for col in extra_columns},
            }
        )
        obs.append(o)
        if len(obs) > 1000:
            db.session.bulk_insert_mappings(ImportSyntheseData, obs)
            obs = []
    if obs:
        db.session.bulk_insert_mappings(ImportSyntheseData, obs)
    return line_no


def load_import_data_in_dataframe(imprt, fields):
    source_cols = [
        "id_import",
        "line_no",
        "valid",
    ] + [field.source_column for field in fields.values()]
    records = (
        db.session.query(*[ImportSyntheseData.__table__.c[col] for col in source_cols])
        .filter(
            ImportSyntheseData.imprt == imprt,
        )
        .all()
    )
    df = pd.DataFrame.from_records(
        records,
        columns=source_cols,
    )
    return df


def update_import_data_from_dataframe(imprt, fields, df):
    db.session.query(ImportSyntheseData).filter_by(id_import=imprt.id_import).update(
        {"valid": False}
    )
    if not len(df[df["valid"] == True]):
        return
    updated_cols = [
        "id_import",
        "line_no",
        "valid",
    ]
    updated_cols += [
        field.synthese_field for field in fields.values() if field.synthese_field
    ]
    df.replace({np.nan: None, pd.NaT: None}, inplace=True)
    records = df[df["valid"] == True][updated_cols].to_dict(orient="records")
    insert_stmt = pg_insert(ImportSyntheseData)
    insert_stmt = insert_stmt.values(records).on_conflict_do_update(
        index_elements=updated_cols[:2],
        set_={col: insert_stmt.excluded[col] for col in updated_cols[2:]},
    )
    db.session.execute(insert_stmt)


def toggle_synthese_triggers(enable):
    triggers = ["tri_meta_dates_change_synthese", "tri_insert_cor_area_synthese"]
    action = "ENABLE" if enable else "DISABLE"
    with db.session.begin_nested():
        for trigger in triggers:
            db.session.execute(
                f"ALTER TABLE gn_synthese.synthese {action} TRIGGER {trigger}"
            )


def import_data_to_synthese(imprt, source):
    generated_fields = {
        "datetime_min",
        "datetime_max",
        "the_geom_4326",
        "the_geom_local",
        "the_geom_point",
        "id_area_attachment",
    }
    if imprt.fieldmapping.get("unique_id_sinp_generate", False):
        generated_fields |= {"unique_id_sinp"}
    if imprt.fieldmapping.get("altitudes_generate", False):
        generated_fields |= {"altitude_min", "altitude_max"}
    fields = BibFields.query.filter(
        BibFields.synthese_field != None,
        BibFields.name_field.in_(imprt.fieldmapping.keys() | generated_fields),
    ).all()
    select_stmt = (
        ImportSyntheseData.query.filter_by(imprt=imprt, valid=True)
        .with_entities(
            *[getattr(ImportSyntheseData, field.synthese_field) for field in fields]
        )
        .add_columns(
            literal(source.id_source),
            literal(TModules.query.filter_by(module_code=MODULE_CODE).one().id_module),
            literal(imprt.id_dataset),
            literal("I"),
        )
    )
    names = [field.synthese_field for field in fields] + [
        "id_source",
        "id_module",
        "id_dataset",
        "last_action",
    ]
    insert_stmt = insert(Synthese).from_select(
        names=names,
        select=select_stmt,
    )
    db.session.execute(insert_stmt)


def populate_cor_area_synthese(imprt, source):
    # Populate synthese / area association table
    # A synthese entry is associated to an area when the area is enabled,
    # and when the synthese geom intersects with the area
    # (we also check the intersection is more than just touches when the geom is not a point)
    synthese_geom = Synthese.__table__.c.the_geom_local
    area_geom = LAreas.__table__.c.geom
    stmt = corAreaSynthese.insert().from_select(
        names=[
            corAreaSynthese.c.id_synthese,
            corAreaSynthese.c.id_area,
        ],
        select=select(
            [
                Synthese.__table__.c.id_synthese,
                LAreas.__table__.c.id_area,
            ]
        )
        .select_from(
            Synthese.__table__.join(
                LAreas.__table__,
                sa.func.ST_Intersects(synthese_geom, area_geom),
            )
        )
        .where(
            (LAreas.__table__.c.enable == True)
            & (
                (sa.func.ST_GeometryType(synthese_geom) == "ST_Point")
                | ~(sa.func.ST_Touches(synthese_geom, area_geom))
            )
            & (Synthese.__table__.c.id_source == source.id_source)
        ),
    )
    db.session.execute(stmt)

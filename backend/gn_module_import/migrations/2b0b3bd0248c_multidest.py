"""multidest

Revision ID: 2b0b3bd0248c
Revises: 2896cf965dd6
Create Date: 2023-10-20 09:05:49.973738

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.schema import Table, MetaData


# revision identifiers, used by Alembic.
revision = "2b0b3bd0248c"
down_revision = "2896cf965dd6"
branch_labels = None
depends_on = None


def upgrade():
    meta = MetaData(bind=op.get_bind())
    module = Table("t_modules", meta, autoload=True, schema="gn_commons")
    destination = op.create_table(
        "bib_destinations",
        sa.Column("id_destination", sa.Integer, primary_key=True),
        sa.Column(
            "id_module",
            sa.Integer,
            sa.ForeignKey("gn_commons.t_modules.id_module", ondelete="CASCADE"),
        ),
        sa.Column("code", sa.String(64), unique=True),
        sa.Column("label", sa.String(128)),
        sa.Column("table_name", sa.String(64)),
        schema="gn_imports",
    )
    id_synthese_module = (
        op.get_bind()
        .execute(sa.select([module.c.id_module]).where(module.c.module_code == "SYNTHESE"))
        .scalar()
    )
    op.bulk_insert(
        destination,
        [
            {
                "id_module": id_synthese_module,
                "code": "synthese",
                "label": "synthèse",
                "table_name": "t_imports_synthese",
            },
        ],
    )
    id_synthese_dest = (
        op.get_bind()
        .execute(
            sa.select([destination.c.id_destination]).where(
                destination.c.id_module == id_synthese_module
            )
        )
        .scalar()
    )
    op.add_column(
        "bib_fields",
        sa.Column(
            "id_destination",
            sa.Integer,
            sa.ForeignKey("gn_imports.bib_destinations.id_destination"),
            nullable=True,
        ),
        schema="gn_imports",
    )
    field = Table("bib_fields", meta, autoload=True, schema="gn_imports")
    op.execute(field.update().values({"id_destination": id_synthese_dest}))
    op.alter_column(
        table_name="bib_fields", column_name="id_destination", nullable=False, schema="gn_imports"
    )
    op.add_column(
        "t_imports",
        sa.Column(
            "id_destination",
            sa.Integer,
            sa.ForeignKey("gn_imports.bib_destinations.id_destination"),
            nullable=True,
        ),
        schema="gn_imports",
    )
    imprt = Table("t_imports", meta, autoload=True, schema="gn_imports")
    op.execute(imprt.update().values({"id_destination": id_synthese_dest}))
    op.alter_column(
        table_name="t_imports", column_name="id_destination", nullable=False, schema="gn_imports"
    )
    op.add_column(
        "t_mappings",
        sa.Column(
            "id_destination",
            sa.Integer,
            sa.ForeignKey("gn_imports.bib_destinations.id_destination"),
            nullable=True,
        ),
        schema="gn_imports",
    )
    mapping = Table("t_mappings", meta, autoload=True, schema="gn_imports")
    op.execute(mapping.update().values({"id_destination": id_synthese_dest}))
    op.alter_column(
        table_name="t_mappings", column_name="id_destination", nullable=False, schema="gn_imports"
    )
    entity = op.create_table(
        "bib_entities",
        sa.Column("id_entity", sa.Integer, primary_key=True),
        sa.Column("id_destination", sa.Integer, sa.ForeignKey(destination.c.id_destination)),
        sa.Column("label", sa.String(64)),
        sa.Column("order", sa.Integer),
        sa.Column("validity_column", sa.String(64)),
        schema="gn_imports",
    )
    op.bulk_insert(
        entity,
        [
            {
                "id_destination": id_synthese_dest,
                "label": "Observations",
                "order": 1,
                "validity_column": "gn_is_valid",
            },
        ],
    )
    op.create_table(
        "cor_entity_field",
        sa.Column(
            "id_entity",
            sa.Integer,
            sa.ForeignKey("gn_imports.bib_entities.id_entity", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "id_field",
            sa.Integer,
            sa.ForeignKey("gn_imports.bib_fields.id_field", ondelete="CASCADE"),
            primary_key=True,
        ),
        schema="gn_imports",
    )
    op.execute(
        """
        INSERT INTO
            gn_imports.cor_entity_field (id_entity, id_field)
        SELECT
            e.id_entity,
            f.id_field
        FROM
            gn_commons.t_modules m
        JOIN
            gn_imports.bib_destinations d ON d.id_module = m.id_module
        JOIN
            gn_imports.bib_entities e ON e.id_destination = d.id_destination
        JOIN
            gn_imports.bib_fields f ON f.id_destination = d.id_destination
        WHERE m.module_code = 'SYNTHESE';
        """
    )
    op.execute(
        """
        INSERT INTO
            gn_permissions.t_permissions_available (id_module, id_object, id_action, label, scope_filter)
        SELECT
            m.id_module, o.id_object, a.id_action, 'Importer des observations', TRUE
        FROM
            gn_commons.t_modules m,
            gn_permissions.t_objects o,
            gn_permissions.bib_actions a
        WHERE
            m.module_code = 'SYNTHESE'
            AND
            o.code_object = 'ALL'
            AND
            a.code_action = 'C'
        """
    )
    op.execute(
        """
        INSERT INTO
            gn_permissions.t_permissions (id_role, id_module, id_object, id_action, scope_value)
        SELECT
            p.id_role, new_module.id_module, new_object.id_object, p.id_action, p.scope_value
        FROM
            gn_permissions.t_permissions p
                JOIN gn_permissions.bib_actions a USING(id_action)
                JOIN gn_commons.t_modules m USING(id_module)
                JOIN gn_permissions.t_objects o USING(id_object)
                JOIN utilisateurs.t_roles r USING(id_role),
            gn_commons.t_modules new_module,
            gn_permissions.t_objects new_object
        WHERE
            a.code_action = 'C' AND m.module_code = 'IMPORT' AND o.code_object = 'IMPORT'
            AND
            new_module.module_code = 'SYNTHESE' AND new_object.code_object = 'ALL';
        """
    )
    # TODO unique constraint


def downgrade():
    op.execute(
        """
        DELETE FROM
            gn_permissions.t_permissions p
        USING
            gn_permissions.bib_actions a,
            gn_commons.t_modules m,
            gn_permissions.t_objects o
        WHERE
            p.id_action = a.id_action AND a.code_action = 'C'
            AND
            p.id_module = m.id_module AND m.module_code = 'SYNTHESE'
            AND
            p.id_object = o.id_object AND o.code_object = 'ALL';
        """
    )
    op.execute(
        """
        DELETE FROM
            gn_permissions.t_permissions_available pa
        USING
            gn_permissions.bib_actions a,
            gn_commons.t_modules m,
            gn_permissions.t_objects o
        WHERE
            pa.id_action = a.id_action AND a.code_action = 'C'
            AND
            pa.id_module = m.id_module AND m.module_code = 'SYNTHESE'
            AND
            pa.id_object = o.id_object AND o.code_object = 'ALL';
        """
    )
    op.drop_table("cor_entity_field", schema="gn_imports")
    op.drop_table("bib_entities", schema="gn_imports")
    op.drop_column(schema="gn_imports", table_name="bib_fields", column_name="id_destination")
    op.drop_column(schema="gn_imports", table_name="t_mappings", column_name="id_destination")
    op.drop_column(schema="gn_imports", table_name="t_imports", column_name="id_destination")
    op.drop_table("bib_destinations", schema="gn_imports")

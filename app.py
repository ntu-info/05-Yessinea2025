# app.py
from flask import Flask, jsonify, abort, send_file, Response
import os
import json
from sqlalchemy import create_engine, text
from collections import OrderedDict

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL environment variable.")
    # Normalize old 'postgres://' scheme to 'postgresql://'
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(
        db_url,
        pool_pre_ping=True
    )
    return _engine

def normalize_term(term: str) -> str:
    prefix = "terms_abstract_tfidf__"
    term = term.strip().lower()
    if not term.startswith(prefix):
        term = prefix + term
    return term

def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/test_db", endpoint="test_db")
    def test_db():
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                rows = conn.execute(text(
                    "SELECT study_id, ST_X(geom) AS x, ST_Y(geom) AS y, ST_Z(geom) AS z FROM ns.coordinates LIMIT 3"
                )).mappings().all()
                payload["coordinates_sample"] = [dict(r) for r in rows]

                rows = conn.execute(text("SELECT * FROM ns.metadata LIMIT 3")).mappings().all()
                payload["metadata_sample"] = [dict(r) for r in rows]

                rows = conn.execute(text(
                    "SELECT study_id, contrast_id, term, weight FROM ns.annotations_terms LIMIT 3"
                )).mappings().all()
                payload["annotations_terms_sample"] = [dict(r) for r in rows]

            payload["ok"] = True
            return jsonify(payload), 200
        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    @app.get("/terms_sample")
    def get_terms_sample():
        eng = get_engine()
        with eng.begin() as conn:
            rows = conn.execute(text("SELECT DISTINCT term FROM ns.annotations_terms LIMIT 100000")).fetchall()
            return jsonify([r[0] for r in rows])

    @app.get("/terms/<term>/studies")
    def get_studies_by_term(term):
        eng = get_engine()
        term = normalize_term(term)
        try:
            with eng.begin() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT s.study_id, m.title
                    FROM ns.annotations_terms s
                    JOIN ns.metadata m USING (study_id)
                    WHERE s.term = :term
                    LIMIT 10000;
            """), {"term": term}).mappings().all()
            return jsonify([dict(r) for r in rows])
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/dissociate/terms/<term_a>/<term_b>")
    def functional_dissociation(term_a, term_b):
        eng = get_engine()

        term_a_display = term_a.replace("_", " ").strip().lower()
        term_b_display = term_b.replace("_", " ").strip().lower()
        
        term_a_db = normalize_term(term_a_display)
        term_b_db = normalize_term(term_b_display)

        try:
            with eng.begin() as conn:
                rows_a = conn.execute(text("""
                    SELECT DISTINCT m.study_id, m.title
                    FROM ns.annotations_terms a
                    JOIN ns.metadata m ON a.study_id = m.study_id
                    WHERE a.term = :term_a
                    AND NOT EXISTS (
                        SELECT 1 FROM ns.annotations_terms b
                        WHERE b.study_id = a.study_id AND b.term = :term_b
                    )
                    LIMIT 1000;
                """), {"term_a": term_a_db, "term_b": term_b_db}).mappings().all()

                rows_b = conn.execute(text("""
                    SELECT DISTINCT m.study_id, m.title
                    FROM ns.annotations_terms b
                    JOIN ns.metadata m ON b.study_id = m.study_id
                    WHERE b.term = :term_b
                    AND NOT EXISTS (
                        SELECT 1 FROM ns.annotations_terms a
                        WHERE a.study_id = b.study_id AND a.term = :term_a
                    )
                    LIMIT 1000;
                """), {"term_a": term_a_db, "term_b": term_b_db}).mappings().all()

            return Response(
                json.dumps(OrderedDict([
                    ("term_a", term_a_display),
                    ("term_b", term_b_display),
                    ("term_a_not_term_b", [dict(r) for r in rows_a]),
                    ("term_b_not_term_a", [dict(r) for r in rows_b]),
                ])),
                mimetype="application/json"
            )

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/intersect/terms/<term_a>/<term_b>")
    def intersect_terms(term_a, term_b):
        eng = get_engine()

        term_a_display = term_a.replace("_", " ").strip().lower()
        term_b_display = term_b.replace("_", " ").strip().lower()
        
        term_a_db = normalize_term(term_a_display)
        term_b_db = normalize_term(term_b_display)

        try:
            with eng.begin() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT m.study_id, m.title
                    FROM ns.annotations_terms a
                    JOIN ns.annotations_terms b ON a.study_id = b.study_id
                    JOIN ns.metadata m ON a.study_id = m.study_id
                    WHERE a.term = :term_a
                        AND b.term = :term_b
                    LIMIT 100;
                """), {"term_a": term_a_db, "term_b": term_b_db}).mappings().all()

            return Response(
                json.dumps(OrderedDict([
                    ("term_a", term_a_display),
                    ("term_b", term_b_display),
                    ("term_a_and_term_b", [dict(r) for r in rows]),
                ])),
                mimetype="application/json"
            )

        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    @app.get("/dissociate/locations/<coord1>/<coord2>")
    def dissociate_coordinates(coord1, coord2):
        eng = get_engine()

        try:
            x1, y1, z1 = map(int, coord1.split("_"))
            x2, y2, z2 = map(int, coord2.split("_"))
        except ValueError:
            return jsonify({"error": "Coordinates must be in x_y_z format and integers."}), 400

        try:
            with eng.begin() as conn:
                rows_1_not_2 = conn.execute(text("""
                    SELECT DISTINCT m.study_id, m.title
                    FROM ns.coordinates c1
                    JOIN ns.metadata m ON c1.study_id = m.study_id
                    WHERE ST_X(c1.geom) = :x1 AND ST_Y(c1.geom) = :y1 AND ST_Z(c1.geom) = :z1
                    AND NOT EXISTS (
                        SELECT 1 FROM ns.coordinates c2
                        WHERE c2.study_id = c1.study_id
                        AND ST_X(c2.geom) = :x2 AND ST_Y(c2.geom) = :y2 AND ST_Z(c2.geom) = :z2
                    )
                    LIMIT 100;
                """), {
                    "x1": x1, "y1": y1, "z1": z1,
                    "x2": x2, "y2": y2, "z2": z2
                }).mappings().all()

                rows_2_not_1 = conn.execute(text("""
                    SELECT DISTINCT m.study_id, m.title
                    FROM ns.coordinates c2
                    JOIN ns.metadata m ON c2.study_id = m.study_id
                    WHERE ST_X(c2.geom) = :x2 AND ST_Y(c2.geom) = :y2 AND ST_Z(c2.geom) = :z2
                    AND NOT EXISTS (
                        SELECT 1 FROM ns.coordinates c1
                        WHERE c1.study_id = c2.study_id
                        AND ST_X(c1.geom) = :x1 AND ST_Y(c1.geom) = :y1 AND ST_Z(c1.geom) = :z1
                    )
                    LIMIT 100;
                """), {
                    "x1": x1, "y1": y1, "z1": z1,
                    "x2": x2, "y2": y2, "z2": z2
                }).mappings().all()

            return Response(
                json.dumps(OrderedDict([
                    ("coordinate_1", f"{x1},{y1},{z1}"),
                    ("coordinate_2", f"{x2},{y2},{z2}"),
                    ("coordinate_1_not_coordinate_2", [dict(r) for r in rows_1_not_2]),
                    ("coordinate_2_not_coordinate_1", [dict(r) for r in rows_2_not_1]),
                ])),
                mimetype="application/json"
            )

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return app

app = create_app()

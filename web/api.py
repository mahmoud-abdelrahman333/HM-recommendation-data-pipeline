from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import psycopg2

app = FastAPI(title="H&M Recommendation System")

DB_CONFIG = {
    "host": "postgres",
    "database": "hm_recommendations",
    "user": "hm_user",
    "password": "hm_password",
    "port": 5432
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@app.get("/api/recommendations/{customer_id}")
def get_recommendations(customer_id: str):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            article_id,
            prod_name,
            product_type_name,
            product_group_name,
            department_name,
            section_name,
            recommendation_rank,
            prediction
        FROM recommendations
        WHERE customer_id = %s
        ORDER BY recommendation_rank
        LIMIT 10
    """, (customer_id,))

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return {
        "customer_id": customer_id,
        "recommendations": [
            {
                "article_id": row[0],
                "prod_name": row[1],
                "product_type": row[2],
                "product_group": row[3],
                "department": row[4],
                "section": row[5],
                "rank": row[6],
                "prediction": float(row[7])
            }
            for row in rows
        ]
    }


@app.get("/api/health")
def health():
    return {"status": "ok"}


app.mount("/", StaticFiles(directory="static", html=True), name="static")

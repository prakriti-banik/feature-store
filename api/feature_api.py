from fastapi import FastAPI
import psycopg2
from pydantic import BaseModel
import uvicorn
import redis
import json

app = FastAPI()

class FeatureRequest(BaseModel):
    user_id: str

def init_db():
    conn = psycopg2.connect(
        dbname="feature_store",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_features (
            user_id VARCHAR(50) PRIMARY KEY,
            age INTEGER,
            total_transactions FLOAT,
            transaction_count INTEGER,
            avg_transaction_amount FLOAT,
            last_transaction_timestamp TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()

@app.get("/features/{user_id}")
async def get_features(user_id: str):
    # Try Redis first for low-latency access
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    cached_feature = redis_client.get(f"feature:{user_id}")
    
    if cached_feature:
        redis_client.close()
        return json.loads(cached_feature)

    # Fallback to PostgreSQL
    conn = psycopg2.connect(
        dbname="feature_store",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT user_id, age, total_transactions, transaction_count, avg_transaction_amount, last_transaction_timestamp
        FROM user_features WHERE user_id = %s
    """, (user_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result:
        feature = {
            "user_id": result[0],
            "age": result[1],
            "avg_transaction_amount": result[4],
            "last_transaction_timestamp": result[5].isoformat()
        }
        # Cache in Redis for future requests
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        redis_client.set(f"feature:{user_id}", json.dumps(feature))
        redis_client.close()
        return feature
    return {"error": "User not found"}

if __name__ == "__main__":
    init_db()  # Initialize DB when starting the API
    uvicorn.run(app, host="0.0.0.0", port=8000)
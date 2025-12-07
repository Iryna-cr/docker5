from flask import Flask, request, jsonify
import redis
import json
import psycopg2

app = Flask(__name__)

r = redis.Redis(host="redis", port=6379, decode_responses=True)

def db():
    return psycopg2.connect(
        host="db",
        database="mydb",
        user="postgres",
        password="password"
    )

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/products")
def get_products():

    cached = r.get("products")
    if cached:
        return jsonify(json.loads(cached))

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id,name,price FROM products;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    products = [{"id":i,"name":n,"price":p} for (i,n,p) in rows]

    r.setex("products", 30, json.dumps(products))

    return jsonify(products)

@app.post("/products")
def create():

    data = request.json
    if not data["name"] or data["price"] <= 0:
        return {"error":"invalid"},400

    conn = db()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO products(name,price) VALUES (%s,%s) RETURNING id",
        (data["name"], data["price"])
    )
    pid = cur.fetchone()[0]
    conn.commit()

    cur.close()
    conn.close()

    r.delete("products")

    return {"id": pid}, 201



# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch openai databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Reasoning Agent: Explainable Risk Score
# MAGIC Analyzes transaction metadata + Vector Search similar fraud signatures → plain-English explanation + risk_score 0–100 (GDPR/CCPA explainability).

# COMMAND ----------

import os
import json
from typing import Any, Dict, List, Optional

from databricks.vector_search.client import VectorSearchClient
from openai import OpenAI

CATALOG = "Financial_Security"
SCHEMA = "silver"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.fraud_signatures_index"
ENDPOINT_NAME = "fraud-detection-vs-endpoint"
SERVING_ENDPOINT = os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4-5")

# COMMAND ----------

def get_openai_client():
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    host = w.config.host
    tok = w.config.authenticate()
    if isinstance(tok, dict) and "Authorization" in tok:
        tok = tok["Authorization"].replace("Bearer ", "")
    return OpenAI(api_key=tok, base_url=f"{host}/serving-endpoints")

def transaction_to_query(txn: Dict[str, Any]) -> str:
    parts = []
    if txn.get("transaction_type"):
        parts.append(f"Transaction type: {txn['transaction_type']}")
    if txn.get("amount") is not None:
        parts.append(f"Amount: {txn['amount']}")
    if txn.get("mfa_change_flag"):
        parts.append("MFA or security changed recently.")
    if txn.get("ip_changed_recently"):
        parts.append("IP address changed recently.")
    if txn.get("high_value_wire"):
        parts.append("High-value wire transfer.")
    return " ".join(parts) or "Transaction with no extra signals."

def search_signatures(query_text: str, num: int = 5) -> List[Dict]:
    vs = VectorSearchClient()
    idx = vs.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)
    out = idx.similarity_search(query_text=query_text, columns=["signature_id", "signature_type", "description", "risk_weight"], num_results=num)
    docs = (out.get("result") or out).get("data_array", out.get("rows", [])) or []
    result = []
    for row in docs:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            vals, score = row[0], row[1]
            if isinstance(vals, (list, tuple)) and len(vals) >= 4:
                result.append({"signature_id": vals[0], "signature_type": vals[1], "description": vals[2], "risk_weight": vals[3], "similarity_score": score})
        elif isinstance(row, dict):
            result.append({**row, "similarity_score": row.get("score", 0)})
    return result

def compute_risk_with_llm(transaction_summary: str, similar: List[Dict], client: Optional[OpenAI] = None) -> Dict[str, Any]:
    if client is None:
        client = get_openai_client()
    system = """You are a fraud analyst. Given a transaction summary and similar known fraud signatures, output a JSON object with:
- "risk_score": number 0–100
- "risk_level": one of LOW, MEDIUM, HIGH, CRITICAL
- "reason": plain-English explanation for the score (for regulatory explainability)
- "matched_signatures": list of signature_type(s) that influenced the score. Be concise and human-readable."""
    user = f"Transaction:\n{transaction_summary}\n\nSimilar fraud signatures:\n{json.dumps(similar, indent=2)}\n\nRespond with only one JSON object."
    r = client.chat.completions.create(model=SERVING_ENDPOINT, messages=[{"role": "system", "content": system}, {"role": "user", "content": user}], max_tokens=500, temperature=0.2)
    content = r.choices[0].message.content.strip()
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
    return json.loads(content)

def run_reasoning_agent(txn: Dict[str, Any]) -> Dict[str, Any]:
    query = transaction_to_query(txn)
    similar = search_signatures(query)
    result = compute_risk_with_llm(query, similar)
    result["transaction_query_text"] = query
    result["signatures_searched"] = len(similar)
    return result

# COMMAND ----------

# Example
sample = {"transaction_id": "TXN-001", "user_id": "U1", "amount": 15000, "transaction_type": "wire", "mfa_change_flag": True, "ip_changed_recently": True, "high_value_wire": True}
print(json.dumps(run_reasoning_agent(sample), indent=2))

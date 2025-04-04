import requests
from bs4 import BeautifulSoup
import socket
import pandas as pd
import httpx
from pydantic import BaseModel as PydanticBaseModel, IPvAnyAddress, ValidationError
import psycopg2
from urllib.parse import urlparse
import asyncio
from datetime import datetime
import json
from fastapi import FastAPI, HTTPException
from typing import List, Dict, Optional
import os
from urllib.parse import urlparse
from fastapi.middleware.cors import CORSMiddleware  # Import CORSMiddleware

app = FastAPI(title="Domain Metadata API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins (you can restrict this later)
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

class UrlInput(PydanticBaseModel):
    url: str

class DomainMetadata(PydanticBaseModel):
    domain: str
    ip_addresses: List[IPvAnyAddress]
    tranco_rank: str
    http_details: Dict

try:
    tranco_df = pd.read_csv("top-1m.csv", names=["rank", "domain"], header=None)
    print("Tranco list loaded successfully")
except Exception as e:
    print(f"Error loading Tranco list: {str(e)}")
    raise Exception("Failed to load Tranco list")

def extract_domains(url: str) -> List[str]:
    try:
        print(f"Extracting domains from {url}")
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        domains = set()
        for link in soup.find_all('a', href=True):
            href = link['href']
            parsed = urlparse(href)
            if parsed.netloc and parsed.netloc != urlparse(url).netloc:
                domains.add(parsed.netloc)
        domains.add(urlparse(url).netloc)
        print(f"Extracted domains: {domains}")
        return list(domains)
    except Exception as e:
        print(f"Error extracting domains from {url}: {str(e)}")
        raise

def resolve_ips(domain: str) -> List[str]:
    try:
        print(f"Resolving IPs for {domain}")
        addr_info = socket.getaddrinfo(domain, None, 0, 0, socket.SOL_TCP)
        ips = list(set([info[4][0] for info in addr_info]))
        print(f"Resolved IPs: {ips}")
        return ips
    except socket.gaierror as e:
        print(f"DNS resolution failed for {domain}: {str(e)}")
        return []

def check_tranco_rank(domain: str) -> str:
    try:
        print(f"Checking Tranco rank for {domain}")
        match = tranco_df[tranco_df['domain'] == domain]
        rank = str(match['rank'].values[0]) if not match.empty else "Not in top 1M"
        print(f"Tranco rank: {rank}")
        return rank
    except Exception as e:
        print(f"Error checking Tranco rank for {domain}: {str(e)}")
        raise

async def analyze_http(domain: str) -> Dict:
    async with httpx.AsyncClient(follow_redirects=True, timeout=10) as client:
        protocols = ['https', 'http']
        for proto in protocols:
            url = f"{proto}://{domain}"
            try:
                print(f"Analyzing HTTP for {url}")
                response = await client.get(url)
                headers = dict(response.headers)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    title_tag = soup.title
                    title = title_tag.string if title_tag else "N/A"
                else:
                    title = "N/A"
                http_data = {
                    "protocol": proto,
                    "status": response.status_code,
                    "headers": headers,
                    "cookies": headers.get("set-cookie", "None"),
                    "title": title
                }
                print(f"HTTP analysis result: {http_data}")
                return http_data
            except httpx.RequestError as e:
                print(f"HTTP request failed for {url}: {str(e)}")
                continue
        return {"protocol": "none", "status": "unreachable", "headers": {}, "cookies": "None", "title": "N/A"}

def validate_metadata(domain: str, ips: List[str], rank: str, http_data: Dict) -> Optional[Dict]:
    try:
        print(f"Validating metadata for {domain}")
        metadata = DomainMetadata(
            domain=domain,
            ip_addresses=ips,
            tranco_rank=rank,
            http_details=http_data
        )
        validated_metadata = metadata.dict()
        print(f"Validated metadata: {validated_metadata}")
        return validated_metadata
    except ValidationError as e:
        print(f"Validation error for {domain}: {e}")
        return None

def get_db_connection():
    database_url = os.getenv("DATABASE_URL", "postgresql://RimBay_owner:dHeZ0FVCyrw8@ep-muddy-glade-a5je4e4s-pooler.us-east-2.aws.neon.tech/RimBay?sslmode=require")
    parsed_url = urlparse(database_url)
    return psycopg2.connect(
        dbname="RimBay",  # Supprime le '/' initial
        user=parsed_url.username,
        password=parsed_url.password,
        host=parsed_url.hostname,
        port=parsed_url.port if parsed_url.port else 5432,
        sslmode="require"  # Nécessaire pour Neon
    )

# Mise à jour de store_in_postgres
def store_in_postgres(metadata: Dict):
    try:
        print(f"Connecting to PostgreSQL for {metadata['domain']}")
        conn = get_db_connection()
        cur = conn.cursor()

        ip_addresses_str = [str(ip) for ip in metadata['ip_addresses']]
        http_details = metadata['http_details'].copy()
        if 'headers' in http_details:
            http_details['headers'] = dict(http_details['headers'])

        cur.execute("""
            CREATE TABLE IF NOT EXISTS domain_metadata (
                domain TEXT PRIMARY KEY,
                ip_addresses TEXT[],
                tranco_rank TEXT,
                http_details JSONB,
                timestamp TIMESTAMP
            )
        """)

        cur.execute("""
            INSERT INTO domain_metadata 
            (domain, ip_addresses, tranco_rank, http_details, timestamp)
            VALUES (%s, %s::TEXT[], %s, %s::JSONB, %s)
            ON CONFLICT (domain) DO UPDATE SET
                ip_addresses = EXCLUDED.ip_addresses,
                tranco_rank = EXCLUDED.tranco_rank,
                http_details = EXCLUDED.http_details,
                timestamp = EXCLUDED.timestamp
        """, (
            metadata['domain'],
            ip_addresses_str,
            metadata['tranco_rank'],
            json.dumps(http_details, ensure_ascii=False),
            datetime.now()
        ))

        conn.commit()
        print(f"→ Successfully stored {metadata['domain']} in PostgreSQL")

    except Exception as e:
        print(f"!!! ERROR inserting {metadata['domain']}: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'conn' in locals():
            cur.close()
            conn.close()

@app.post("/extract-and-process", response_model=List[Dict])
async def extract_and_process(url_input: UrlInput):
    try:
        domains = extract_domains(url_input.url)
        metadata_list = []

        for domain in domains:
            ips = resolve_ips(domain)
            rank = check_tranco_rank(domain)
            http_data = await analyze_http(domain)
            metadata = validate_metadata(domain, ips, rank, http_data)
            if metadata:
                store_in_postgres(metadata)
                # Convertir les ip_addresses en chaînes pour la réponse
                metadata_response = metadata.copy()
                metadata_response['ip_addresses'] = [str(ip) for ip in metadata['ip_addresses']]
                metadata_list.append(metadata_response)

        return metadata_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing domains: {str(e)}")

@app.get("/domains", response_model=List[Dict])
async def get_domains():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT domain, ip_addresses, tranco_rank, http_details, timestamp FROM domain_metadata")
        rows = cur.fetchall()
        
        result = []
        for row in rows:
            result.append({
                "domain": row[0],
                "ip_addresses": row[1],  # Déjà des chaînes dans la base
                "tranco_rank": row[2],
                "http_details": row[3],  # Pas besoin de json.loads, c'est déjà un dict
                "timestamp": row[4].isoformat()
            })
        
        cur.close()
        conn.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching domains: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
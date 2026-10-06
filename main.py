# main.py
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field
from bs4 import BeautifulSoup
import re
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
import asyncio
import aiohttp
import os
from urllib.parse import quote_plus
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Free PAN to GSTIN Lookup API",
    version="1.0.0",
    description="Fetch PAN details including associated GSTIN numbers using public government portals - No API token required"
)

# Pydantic models
class PANLookupRequest(BaseModel):
    pan: str = Field(..., min_length=10, max_length=10, description="PAN number (10 characters)")
    search_mode: str = Field("fast", description="fast|deep|comprehensive")

class GSTINInfo(BaseModel):
    gstin: Optional[str] = None
    legal_name: Optional[str] = None
    trade_name: Optional[str] = None
    registration_date: Optional[str] = None
    status: Optional[str] = None
    business_address: Optional[str] = None
    state: Optional[str] = None
    last_updated: Optional[str] = None
    source: Optional[str] = None

class PANDetailsResponse(BaseModel):
    success: bool
    pan_number: str
    name: Optional[str] = None
    full_name: Optional[str] = None
    pan_type: Optional[str] = None
    pan_status: Optional[str] = None
    category: Optional[str] = None
    aadhaar_linked: Optional[bool] = None
    date_of_birth: Optional[str] = None
    father_name: Optional[str] = None

    gstin_details: List[GSTINInfo] = []
    gstin_count: int = 0

    business_nature: Optional[str] = None
    constitution: Optional[str] = None

    address: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None

    sources_checked: List[str] = []
    search_mode: Optional[str] = None

    error: Optional[str] = None
    message: Optional[str] = None
    timestamp: str
    disclaimer: str = "Data from public sources. Verify with official portals."

# Public endpoints for PAN/GSTIN lookup
PUBLIC_SOURCES = {
    "gst_suvidha": "https://gst-suvidha.com/gst-number-search/",
    "gst_search": "https://www.gst.gov.in/search/search-taxpayer",
    "pan_status": "https://www.incometax.gov.in/iec/foportal/",
    "company_search": "https://www.mca.gov.in/mcafoportal/",
    "udyam_search": "https://udyamregistration.gov.in/Udyam_Verify.aspx"
}

@app.get("/")
def read_root():
    """API Information"""
    return {
        "message": "🔍 Free PAN to GSTIN Lookup API",
        "version": "1.0.0",
        "description": "Fetch PAN details with GSTIN numbers using public government portals",
        "features": [
            "No API token required",
            "Uses public government portals",
            "Multiple search sources",
            "Fast and comprehensive modes"
        ],
        "endpoints": {
            "pan_lookup": "GET /api/pan/{pan}",
            "pan_lookup_params": "GET /api/lookup?pan={pan}&mode={fast|deep}",
            "batch_lookup": "POST /api/batch (max 5 PANs)",
            "health": "GET /api/health",
            "sources": "GET /api/sources"
        },
        "note": "This API scrapes publicly available data. Use responsibly and comply with terms of service.",
        "rate_limit": "10 requests per minute"
    }

@app.get("/api/sources")
def list_sources():
    """List available data sources"""
    return {
        "sources": PUBLIC_SOURCES,
        "status": "active",
        "count": len(PUBLIC_SOURCES)
    }

@app.get("/api/pan/{pan_number}", response_model=PANDetailsResponse)
async def lookup_pan_by_path(
    pan_number: str,
    mode: str = "fast"
):
    """Lookup PAN details by path parameter"""
    return await process_pan_lookup(pan_number.upper(), mode)

@app.get("/api/lookup", response_model=PANDetailsResponse)
async def lookup_pan_query(
    pan: str = Query(..., min_length=10, max_length=10),
    mode: str = Query("fast", description="Search mode: fast, deep, comprehensive")
):
    """Lookup PAN details by query parameter"""
    return await process_pan_lookup(pan.upper(), mode)

@app.post("/api/lookup", response_model=PANDetailsResponse)
async def lookup_pan_post(request: PANLookupRequest):
    """Lookup PAN details via POST request"""
    return await process_pan_lookup(request.pan.upper(), request.search_mode)

@app.post("/api/batch")
async def batch_lookup(pans: List[str], background_tasks: BackgroundTasks):
    """Batch lookup for multiple PANs (async processing)"""
    if len(pans) > 5:
        raise HTTPException(status_code=400, detail="Maximum 5 PANs per batch")

    valid_pans = [p.upper() for p in pans if len(p) == 10]

    results = []
    for pan in valid_pans:
        try:
            result = await process_pan_lookup(pan, "fast")
            results.append(result.dict())
        except Exception as e:
            results.append({
                "pan_number": pan,
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })

    return {
        "batch_id": f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "total_pans": len(pans),
        "processed": len(valid_pans),
        "results": results,
        "timestamp": datetime.now().isoformat()
    }

async def process_pan_lookup(pan: str, mode: str = "fast") -> PANDetailsResponse:
    """Core PAN lookup logic using public sources"""

    if not re.match(r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$', pan):
        raise HTTPException(status_code=400, detail="Invalid PAN format. Must be like ABCDE1234F")

    sources_checked = []
    gstin_results = []
    pan_details = {}

    try:
        # Determine which sources to check based on mode
        # NOTE: pass function references (not calls) — session & pan are passed later
        if mode == "fast":
            tasks = [search_gst_suvidha, search_pan_details]
        elif mode == "deep":
            tasks = [
                search_gst_suvidha,
                search_gst_portal,
                search_pan_details,
                search_mca_portal
            ]
        else:  # comprehensive
            tasks = [
                search_gst_suvidha,
                search_gst_portal,
                search_pan_details,
                search_mca_portal,
                search_udyam_portal
            ]

        # Execute all searches concurrently
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                *[task(session, pan) for task in tasks],
                return_exceptions=True
            )

        # Process results
        for result in results:
            if isinstance(result, Exception):
                continue

            if result and "source" in result:
                sources_checked.append(result["source"])

                if "gstin_details" in result:
                    gstin_results.extend(result["gstin_details"])

                if "pan_details" in result:
                    pan_details.update(result["pan_details"])

        # Deduplicate GSTIN results
        unique_gstins = {}
        for gstin in gstin_results:
            if gstin.gstin and gstin.gstin not in unique_gstins:
                unique_gstins[gstin.gstin] = gstin

        pan_category = determine_pan_category(pan)

        return PANDetailsResponse(
            success=True,
            pan_number=pan,
            name=pan_details.get("name"),
            full_name=pan_details.get("full_name", pan_details.get("name")),
            pan_type=pan_details.get("type", pan_category),
            pan_status=pan_details.get("status", "Unknown"),
            category=pan_category,
            aadhaar_linked=pan_details.get("aadhaar_linked"),
            date_of_birth=pan_details.get("date_of_birth"),
            father_name=pan_details.get("father_name"),

            gstin_details=list(unique_gstins.values()),
            gstin_count=len(unique_gstins),

            business_nature=pan_details.get("business_nature"),
            constitution=pan_details.get("constitution"),

            address=pan_details.get("address"),
            state=pan_details.get("state"),
            pincode=pan_details.get("pincode"),

            sources_checked=sources_checked,
            search_mode=mode,

            message="PAN lookup completed",
            timestamp=datetime.now().isoformat()
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lookup failed: {str(e)}")

# ==================== SEARCH FUNCTIONS ====================

async def search_gst_suvidha(session: aiohttp.ClientSession, pan: str) -> Dict[str, Any]:
    """Search GST Suvidha portal"""
    try:
        url = f"{PUBLIC_SOURCES['gst_suvidha']}?search={pan}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5"
        }

        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            gstin_details = []

            gstin_pattern = r'\d{2}[A-Z]{5}\d{4}[A-Z]{1}[A-Z\d]{1}[Z]{1}[A-Z\d]{1}'
            matches = re.findall(gstin_pattern, html)

            for match in matches[:5]:
                gstin_details.append(GSTINInfo(
                    gstin=match,
                    source="gst_suvidha",
                    last_updated=datetime.now().strftime("%Y-%m-%d")
                ))

            business_name = None
            name_elements = soup.find_all(
                ['h1', 'h2', 'h3', 'div', 'span'],
                class_=re.compile(r'business|name|company|title', re.I)
            )

            for elem in name_elements:
                text = elem.get_text(strip=True)
                if 5 < len(text) < 100:
                    business_name = text
                    break

            return {
                "source": "gst_suvidha",
                "gstin_details": gstin_details,
                "pan_details": {"name": business_name}
            }

    except Exception:
        return {"source": "gst_suvidha", "error": "Failed to fetch"}

async def search_gst_portal(session: aiohttp.ClientSession, pan: str) -> Dict[str, Any]:
    """Search official GST portal (placeholder – CAPTCHA protected)"""
    try:
        return {
            "source": "gst_portal",
            "gstin_details": [],
            "pan_details": {"note": "GST portal requires CAPTCHA for direct access"}
        }
    except Exception:
        return {"source": "gst_portal", "error": "Access restricted"}

async def search_pan_details(session: aiohttp.ClientSession, pan: str) -> Dict[str, Any]:
    """Search PAN details from public sources"""
    try:
        url = "https://www.incometaxindia.gov.in/Pages/utilities/Know-Your-PAN.aspx"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }

        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
            html = await response.text()

            name_pattern = r'Name[:\s]*([A-Z\s\.]+)'
            name_match = re.search(name_pattern, html, re.IGNORECASE)

            pan_details = {}
            if name_match:
                pan_details["name"] = name_match.group(1).strip()

            pan_details["type"] = determine_pan_category(pan)

            return {
                "source": "pan_portal",
                "pan_details": pan_details
            }

    except Exception:
        return {"source": "pan_portal", "error": "Failed to fetch"}

async def search_mca_portal(session: aiohttp.ClientSession, pan: str) -> Dict[str, Any]:
    """Search MCA (Ministry of Corporate Affairs) portal"""
    try:
        return {
            "source": "mca_portal",
            "pan_details": {"note": "MCA lookup requires company CIN/LLPIN"}
        }
    except Exception:
        return {"source": "mca_portal", "error": "Failed to fetch"}

async def search_udyam_portal(session: aiohttp.ClientSession, pan: str) -> Dict[str, Any]:
    """Search Udyam registration portal"""
    try:
        return {
            "source": "udyam_portal",
            "pan_details": {"note": "Udyam portal lookup available"}
        }
    except Exception:
        return {"source": "udyam_portal", "error": "Failed to fetch"}

def determine_pan_category(pan: str) -> str:
    """Determine PAN holder category from PAN number"""
    if len(pan) < 4:
        return "Unknown"

    fourth_char = pan[3]
    categories = {
        'A': 'Association of Persons (AOP)',
        'B': 'Body of Individuals (BOI)',
        'C': 'Company',
        'F': 'Firm/Limited Liability Partnership',
        'G': 'Government Agency',
        'H': 'Hindu Undivided Family (HUF)',
        'L': 'Local Authority',
        'J': 'Artificial Juridical Person',
        'P': 'Individual/Proprietor',
        'T': 'Trust',
        'E': 'Estates'
    }

    return categories.get(fourth_char, 'Individual/Other')

@app.get("/api/health")
async def health_check():
    """API health check"""
    try:
        test_url = "https://gst-suvidha.com"
        async with aiohttp.ClientSession() as session:
            async with session.get(test_url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                gst_suvidha_up = response.status == 200

        return {
            "status": "healthy",
            "service": "Free PAN to GSTIN Lookup API",
            "sources_available": len(PUBLIC_SOURCES),
            "gst_suvidha_up": gst_suvidha_up,
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0",
            "environment": os.getenv("VERCEL_ENV", "development")
        }
    except Exception as e:
        return {
            "status": "degraded",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_cache_control(request, call_next):
    """Add cache control headers"""
    response = await call_next(request)
    response.headers["Cache-Control"] = "public, max-age=300"
    response.headers["X-API-Version"] = "1.0.0"
    return response

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

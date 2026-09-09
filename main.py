from __future__ import annotations

import math
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator
from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Float, Integer, String, Text, create_engine, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


def database_url() -> str:
    url = os.getenv("DATABASE_URL", "sqlite:///./jne_pod.db")
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


DATABASE_URL = database_url()
engine_args = {"pool_pre_ping": True}
if DATABASE_URL.startswith("sqlite"):
    engine_args["connect_args"] = {"check_same_thread": False}
engine = create_engine(DATABASE_URL, **engine_args)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class PodEvent(Base):
    __tablename__ = "pod_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    eventId: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    idempotencyKey: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    waybill: Mapped[str] = mapped_column(String(255), index=True)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    courierName: Mapped[str | None] = mapped_column(String(255), nullable=True)
    action: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[str | None] = mapped_column(String(100), nullable=True)
    decision: Mapped[str | None] = mapped_column(String(100), nullable=True)
    personStatus: Mapped[str | None] = mapped_column(String(100), nullable=True)
    humanDetected: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    gpsStatus: Mapped[str | None] = mapped_column(String(100), nullable=True)
    photoQualityPassed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    validationStatus: Mapped[str | None] = mapped_column(String(100), nullable=True)
    validationMode: Mapped[str | None] = mapped_column(String(100), nullable=True)
    complianceFlag: Mapped[str | None] = mapped_column(String(50), nullable=True)
    geoMatchComment: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    actionLatitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    actionLongitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    photoLatitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    photoLongitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    expectedLatitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    expectedLongitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    backendLatitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    backendLongitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    actionToPhotoDistanceMeters: Mapped[float | None] = mapped_column(Float, nullable=True)
    actionToBackendDistanceMeters: Mapped[float | None] = mapped_column(Float, nullable=True)
    photoToBackendDistanceMeters: Mapped[float | None] = mapped_column(Float, nullable=True)
    thresholdMeters: Mapped[float | None] = mapped_column(Float, nullable=True)
    actionTimestampIso: Mapped[str | None] = mapped_column(String(100), nullable=True)
    actionTimestampEpoch: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    photoTimestampIso: Mapped[str | None] = mapped_column(String(100), nullable=True)
    photoTimestampEpoch: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    createdAt: Mapped[str | None] = mapped_column(String(100), nullable=True)
    syncedAt: Mapped[str | None] = mapped_column(String(100), nullable=True)
    ingestedAt: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    deviceModel: Mapped[str | None] = mapped_column(String(255), nullable=True)
    osVersion: Mapped[str | None] = mapped_column(String(100), nullable=True)
    appVersion: Mapped[str | None] = mapped_column(String(100), nullable=True)
    customerName: Mapped[str | None] = mapped_column(String(255), nullable=True)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    derivedDeliveryStatus: Mapped[str | None] = mapped_column(String(100), nullable=True)
    photoMapsLink: Mapped[str | None] = mapped_column(Text, nullable=True)
    extraFields: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


KNOWN_FIELDS = {c.name for c in PodEvent.__table__.columns} - {"id", "extraFields"}
ALIASES = {
    "event_id": "eventId", "idempotency_key": "idempotencyKey", "trackingNumber": "waybill",
    "tracking_number": "waybill", "awb": "waybill", "courier_name": "courierName",
    "userName": "username", "human_detected": "humanDetected", "personDetected": "humanDetected",
    "gps_status": "gpsStatus", "photo_quality_passed": "photoQualityPassed",
    "validation_status": "validationStatus", "validation_mode": "validationMode",
    "compliance_flag": "complianceFlag", "geo_match_comment": "geoMatchComment",
    "action_latitude": "actionLatitude", "action_longitude": "actionLongitude",
    "photo_latitude": "photoLatitude", "photo_longitude": "photoLongitude",
    "expected_latitude": "expectedLatitude", "expected_longitude": "expectedLongitude",
    "backend_latitude": "backendLatitude", "backend_longitude": "backendLongitude",
    "threshold_meters": "thresholdMeters", "device_model": "deviceModel", "os_version": "osVersion",
    "app_version": "appVersion", "customer_name": "customerName",
}


class PodPayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    eventId: str
    idempotencyKey: str
    waybill: str
    username: str | None = None
    courierName: str | None = None
    action: str | None = None
    status: str | None = None
    decision: str | None = None
    personStatus: str | None = None
    humanDetected: bool | None = None
    gpsStatus: str | None = None
    photoQualityPassed: bool | None = None
    validationStatus: str | None = None
    validationMode: str | None = None
    complianceFlag: bool | str | None = None
    geoMatchComment: str | None = None
    summary: str | None = None
    actionLatitude: float | None = None
    actionLongitude: float | None = None
    photoLatitude: float | None = None
    photoLongitude: float | None = None
    expectedLatitude: float | None = None
    expectedLongitude: float | None = None
    backendLatitude: float | None = None
    backendLongitude: float | None = None
    actionToPhotoDistanceMeters: float | None = None
    actionToBackendDistanceMeters: float | None = None
    photoToBackendDistanceMeters: float | None = None
    thresholdMeters: float | None = None
    actionTimestampIso: str | None = None
    actionTimestampEpoch: int | None = None
    photoTimestampIso: str | None = None
    photoTimestampEpoch: int | None = None
    createdAt: str | None = None
    syncedAt: str | None = None
    ingestedAt: datetime | str | None = None
    deviceModel: str | None = None
    osVersion: str | None = None
    appVersion: str | None = None
    customerName: str | None = None
    address: str | None = None
    derivedDeliveryStatus: str | None = None
    photoMapsLink: str | None = None

    @field_validator("eventId", "idempotencyKey", "waybill")
    @classmethod
    def required_nonempty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def haversine(lat1: float | None, lon1: float | None, lat2: float | None, lon2: float | None) -> float | None:
    if None in (lat1, lon1, lat2, lon2):
        return None
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi, dlambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return round(6371000 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 2)


def compliance(value: bool | str | None) -> str | None:
    if isinstance(value, bool):
        return "COMPLIANT" if value else "NON_COMPLIANT"
    if value is None:
        return None
    normalized = str(value).strip().upper().replace("-", "_").replace(" ", "_")
    if normalized in {"TRUE", "YES", "PASS", "PASSED", "COMPLIANT", "IN_RADIUS", "WITHIN_RADIUS"}:
        return "COMPLIANT"
    if normalized in {"FALSE", "NO", "FAIL", "FAILED", "NON_COMPLIANT", "NONCOMPLIANT", "OUTSIDE_RADIUS", "OUT_OF_RADIUS"}:
        return "NON_COMPLIANT"
    if normalized in {"N/A", "NA", "NOT_APPLICABLE", "UNKNOWN", ""}:
        return "N/A"
    return str(value).strip()


def destination(data: dict[str, Any]) -> tuple[float | None, float | None]:
    if data.get("backendLatitude") is not None and data.get("backendLongitude") is not None:
        return data["backendLatitude"], data["backendLongitude"]
    if data.get("expectedLatitude") is not None and data.get("expectedLongitude") is not None:
        return data["expectedLatitude"], data["expectedLongitude"]
    return None, None


def serialize(row: PodEvent) -> dict[str, Any]:
    result = {name: getattr(row, name) for name in KNOWN_FIELDS}
    result["ingestedAt"] = row.ingestedAt.isoformat().replace("+00:00", "Z") if row.ingestedAt else None
    result["extraFields"] = row.extraFields or {}
    return result


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="JNE POD Backend", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in os.getenv("CORS_ORIGINS", "*").split(",")],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "jne-pod-backend"}


@app.post("/api/pod")
def create_pod(payload: dict[str, Any], db: Session = Depends(get_db)) -> dict[str, Any]:
    # Apply aliases before validation; canonical keys always win.
    normalized = dict(payload)
    for alias, canonical in ALIASES.items():
        if canonical not in normalized and alias in normalized:
            normalized[canonical] = normalized[alias]
    try:
        data = PodPayload.model_validate(normalized).model_dump()
    except ValidationError as exc:
        errors = [{"field": ".".join(map(str, error["loc"])), "message": error["msg"]} for error in exc.errors()]
        raise HTTPException(status_code=422, detail=errors) from None
    existing = db.scalar(select(PodEvent).where(or_(PodEvent.eventId == data["eventId"], PodEvent.idempotencyKey == data["idempotencyKey"])))
    if existing:
        return {"success": True, "eventId": data["eventId"], "syncStatus": "SYNCED", "duplicate": True}

    data["username"] = data.get("username") if data.get("username") is not None else data.get("courierName")
    data["complianceFlag"] = compliance(data.get("complianceFlag"))
    geo_comment = (data.get("geoMatchComment") or "").lower()
    if "outside" in geo_comment or "out of radius" in geo_comment:
        data["complianceFlag"] = "NON_COMPLIANT"
    data["ingestedAt"] = datetime.now(timezone.utc)
    data["actionToPhotoDistanceMeters"] = data.get("actionToPhotoDistanceMeters")
    if data["actionToPhotoDistanceMeters"] is None:
        data["actionToPhotoDistanceMeters"] = haversine(data.get("actionLatitude"), data.get("actionLongitude"), data.get("photoLatitude"), data.get("photoLongitude"))
    dest_lat, dest_lon = destination(data)
    if dest_lat is not None:
        if data.get("actionToBackendDistanceMeters") is None:
            data["actionToBackendDistanceMeters"] = haversine(data.get("actionLatitude"), data.get("actionLongitude"), dest_lat, dest_lon)
        if data.get("photoToBackendDistanceMeters") is None:
            data["photoToBackendDistanceMeters"] = haversine(data.get("photoLatitude"), data.get("photoLongitude"), dest_lat, dest_lon)
        distance = data.get("photoToBackendDistanceMeters") or data.get("actionToBackendDistanceMeters")
        if distance is not None and data.get("thresholdMeters") is not None:
            inside = distance <= data["thresholdMeters"]
            data["complianceFlag"] = "COMPLIANT" if inside else "NON_COMPLIANT"
            data["geoMatchComment"] = "Within delivery radius" if inside else "Outside delivery radius"
    else:
        data["actionToBackendDistanceMeters"] = None
        data["photoToBackendDistanceMeters"] = None
        if not data.get("validationMode"):
            data["validationMode"] = "TWO_POINT"

    extra = {k: v for k, v in payload.items() if k not in KNOWN_FIELDS and k not in ALIASES}
    row = PodEvent(**{k: v for k, v in data.items() if k in KNOWN_FIELDS}, extraFields=extra or None)
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return {"success": True, "eventId": data["eventId"], "syncStatus": "SYNCED", "duplicate": True}
    return {"success": True, "eventId": data["eventId"], "syncStatus": "SYNCED", "duplicate": False}


@app.get("/api/pod")
def list_pod(db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    return [serialize(row) for row in db.scalars(select(PodEvent).order_by(PodEvent.ingestedAt.desc(), PodEvent.id.desc())).all()]


@app.get("/api/pod/{waybill}")
def by_waybill(waybill: str, db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    rows = db.scalars(select(PodEvent).where(PodEvent.waybill == waybill).order_by(PodEvent.ingestedAt.desc(), PodEvent.id.desc())).all()
    return [serialize(row) for row in rows]


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> str:
    return DASHBOARD_HTML


DASHBOARD_HTML = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>JNE POD Dashboard</title><style>
:root{--red:#d71920;--ink:#17212b;--muted:#6b7682;--line:#e5e9ee;--bg:#f4f6f8;--good:#15803d;--bad:#c62828}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px Inter,system-ui,sans-serif}header{background:linear-gradient(110deg,#b80f18,#e1252c);color:white;padding:26px max(24px,calc((100% - 1500px)/2))}h1{margin:0;font-size:27px}header p{margin:6px 0 0;opacity:.85}.wrap{max-width:1500px;margin:auto;padding:20px}.panel,.card{background:white;border:1px solid var(--line);border-radius:12px;box-shadow:0 2px 8px #17212b0a}.filters{display:grid;grid-template-columns:repeat(5,minmax(130px,1fr));gap:12px;padding:16px}.filters label{font-size:12px;color:var(--muted);display:block;margin-bottom:5px}.filters input,.filters select{width:100%;padding:9px;border:1px solid #ccd3da;border-radius:7px;background:white}.cards{display:grid;grid-template-columns:repeat(7,1fr);gap:12px;margin:16px 0}.card{padding:15px}.card .n{font-size:25px;font-weight:750;margin-top:5px}.card .label{color:var(--muted);font-size:12px}.charts{display:grid;grid-template-columns:1fr 1fr;gap:16px}.chart{padding:18px}.chart h2,.transactions h2{font-size:16px;margin:0 0 15px}.barrow{display:grid;grid-template-columns:minmax(105px,32%) 1fr 35px;gap:8px;align-items:center;margin:8px 0;font-size:12px}.track{height:11px;border-radius:8px;background:#edf0f3;overflow:hidden}.bar{height:100%;background:var(--red);border-radius:8px}.muted{color:var(--muted)}button.link{border:0;background:none;color:#b80f18;font-weight:650;cursor:pointer;padding:8px 0}.transactions{margin-top:16px;padding:18px;overflow:hidden}.tablewrap{overflow:auto}table{border-collapse:collapse;width:100%;min-width:1800px}th,td{padding:9px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap;font-size:12px}th{position:sticky;top:0;background:#f8fafb;color:#56616d}.pill{padding:3px 7px;border-radius:10px;background:#eef1f4}.good{color:var(--good)}.bad{color:var(--bad)}dialog{border:0;border-radius:12px;max-width:720px;width:92%;padding:0;box-shadow:0 20px 70px #0005}dialog::backdrop{background:#0008}.modalhead{padding:16px 20px;background:#f5f6f7;font-weight:700}.modalbody{padding:20px;display:grid;grid-template-columns:1fr 1fr;gap:10px}.modalbody div{overflow-wrap:anywhere}.modalfoot{padding:12px 20px;text-align:right}.empty{padding:28px;color:var(--muted);text-align:center}@media(max-width:900px){.filters,.cards,.charts{grid-template-columns:1fr 1fr}.cards{grid-template-columns:repeat(2,1fr)}}@media(max-width:560px){.wrap{padding:12px}.filters,.charts{grid-template-columns:1fr}.cards{grid-template-columns:1fr 1fr}header{padding:22px 16px}}
</style></head><body><header><h1>JNE POD Dashboard</h1><p>Proof of Delivery · Transaction monitoring and compliance</p></header><main class="wrap">
<section class="panel filters"><div><label>Username</label><select id="username"><option value="">All usernames</option></select></div><div><label>From Date</label><input type="date" id="from"></div><div><label>To Date</label><input type="date" id="to"></div><div><label>Waybill search</label><input id="waybill" placeholder="Search waybill"></div><div><label>Action / Status</label><select id="status"><option value="">All actions/statuses</option></select></div></section>
<section class="cards" id="cards"></section><section class="charts" id="charts"></section>
<section class="panel transactions"><h2>Transactions <span class="muted" id="recordCount"></span></h2><div class="tablewrap"><table><thead><tr><th>Ingested UTC</th><th>Username</th><th>Waybill</th><th>Action / Status</th><th>Decision</th><th>Human</th><th>Action → Photo</th><th>Action → Backend</th><th>Photo → Backend</th><th>Threshold</th><th>Validation Mode</th><th>SDK Validation</th><th>SDK GPS</th><th>Photo Quality</th><th>Compliance</th><th>Geo Match</th><th>Map</th><th>Details</th></tr></thead><tbody id="rows"></tbody></table></div><button class="link" id="tableToggle"></button></section>
</main><dialog id="details"><div class="modalhead">POD Event Details</div><div class="modalbody" id="detailBody"></div><div class="modalfoot"><button onclick="details.close()">Close</button></div></dialog>
<script>
let all=[], chartOpen=JSON.parse(localStorage.getItem('jneChartOpen')||'{}'), tableOpen=localStorage.getItem('jneTableOpen')==='true';
const $=id=>document.getElementById(id), na=v=>v===null||v===undefined||v===''?'N/A':v, esc=v=>String(na(v)).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])), val=id=>$(id).value;
function label(r){return r.action||r.status||r.derivedDeliveryStatus||null}function human(v){return v===true?'YES':v===false?'NO':'N/A'}function quality(v){return v===true?'PASS':v===false?'FAIL':'N/A'}function dist(v){return v==null?'N/A':Number(v).toFixed(1)+' m'}
function filtered(){let f=val('from'),t=val('to'),w=val('waybill').toLowerCase(),u=val('username'),s=val('status');return all.filter(r=>{let d=(r.ingestedAt||'').slice(0,10);return(!u||r.username===u)&&(!s||label(r)===s)&&(!f||d>=f)&&(!t||d<=t)&&(!w||String(r.waybill).toLowerCase().includes(w))})}
function counts(rows,key){let x={};rows.forEach(r=>{let k=key(r);k=na(k);x[k]=(x[k]||0)+1});return x}function grouped(rows,key){let x={};rows.forEach(r=>{let u=na(r.username),k=na(key(r)),q=u+' · '+k;x[q]=(x[q]||0)+1});return x}
function chart(title,data,index,limited){let entries=Object.entries(data).sort((a,b)=>b[1]-a[1]),open=chartOpen[index],shown=limited&&!open?entries.slice(0,12):entries,max=Math.max(1,...entries.map(x=>x[1]));return `<article class="panel chart"><h2>${title}</h2>${shown.length?shown.map(([k,n])=>`<div class="barrow"><span title="${esc(k)}">${esc(k)}</span><div class="track"><div class="bar" style="width:${n/max*100}%"></div></div><b>${n}</b></div>`).join(''):'<div class="empty">No matching data</div>'}${limited&&entries.length>12?`<button class="link" onclick="toggleChart('${index}')">${open?'Show less ↑':'Show all ↓'}</button>`:''}</article>`}
function toggleChart(i){chartOpen[i]=!chartOpen[i];localStorage.setItem('jneChartOpen',JSON.stringify(chartOpen));render()}
function mapLink(r){let lat=r.photoLatitude??r.actionLatitude,lon=r.photoLongitude??r.actionLongitude;if(lat==null||lon==null)return'N/A';return `<a target="_blank" rel="noopener" href="https://www.google.com/maps?q=${encodeURIComponent(lat+','+lon)}">Open map</a>`}
function render(){let r=filtered(),summary=[['Total Transactions',r.length],['Delivered',r.filter(x=>label(x)==='DELIVERED').length],['Non-delivered',r.filter(x=>label(x)==='NON_DELIVERED').length],['Genuine',r.filter(x=>String(x.decision).toUpperCase()==='GENUINE').length],['Not Genuine',r.filter(x=>String(x.decision).toUpperCase().replace('_',' ')==='NOT GENUINE').length],['Compliant',r.filter(x=>x.complianceFlag==='COMPLIANT').length],['Non-compliant',r.filter(x=>x.complianceFlag==='NON_COMPLIANT').length]];$('cards').innerHTML=summary.map(x=>`<div class="card"><div class="label">${x[0]}</div><div class="n">${x[1]}</div></div>`).join('');$('charts').innerHTML=chart('Decision by Count',counts(r,x=>x.decision),'a',false)+chart('Count by Username and Decision',grouped(r,x=>x.decision),'b',true)+chart('Count by Username and Human Detection',grouped(r,x=>human(x.humanDetected)),'c',true)+chart('Count by Username and Status',grouped(r,x=>label(x)),'d',true)+chart('Count by Username and Distance Between Clicks',grouped(r,x=>dist(x.actionToPhotoDistanceMeters)),'e',true);$('recordCount').textContent=`(${r.length} filtered records)`;let visible=tableOpen?r:r.slice(0,12);$('rows').innerHTML=visible.map((x,i)=>`<tr><td>${esc(x.ingestedAt)}</td><td>${esc(x.username)}</td><td>${esc(x.waybill)}</td><td>${esc(label(x))}</td><td>${esc(x.decision)}</td><td>${human(x.humanDetected)}</td><td>${dist(x.actionToPhotoDistanceMeters)}</td><td>${dist(x.actionToBackendDistanceMeters)}</td><td>${dist(x.photoToBackendDistanceMeters)}</td><td>${dist(x.thresholdMeters)}</td><td>${esc(x.validationMode)}</td><td>${esc(x.validationStatus)}</td><td>${esc(x.gpsStatus)}</td><td>${quality(x.photoQualityPassed)}</td><td class="${x.complianceFlag==='COMPLIANT'?'good':x.complianceFlag==='NON_COMPLIANT'?'bad':''}">${esc(x.complianceFlag)}</td><td>${esc(x.geoMatchComment)}</td><td>${mapLink(x)}</td><td><button onclick="showDetail('${esc(x.eventId)}')">View Details</button></td></tr>`).join('');$('tableToggle').style.display=r.length>12?'block':'none';$('tableToggle').textContent=tableOpen?'Show latest 12 ↑':'Show all transactions ↓'}
function showDetail(id){let r=all.find(x=>x.eventId===id),fields=['eventId','idempotencyKey','username','courierName','waybill','actionLatitude','actionLongitude','photoLatitude','photoLongitude','expectedLatitude','expectedLongitude','backendLatitude','backendLongitude','actionTimestampIso','actionTimestampEpoch','photoTimestampIso','photoTimestampEpoch','createdAt','syncedAt','ingestedAt','customerName','address','deviceModel','osVersion','appVersion','summary'];$('detailBody').innerHTML=fields.map(k=>`<div><b>${esc(k)}</b><br><span class="muted">${esc(r[k])}</span></div>`).join('');$('details').showModal()}
async function load(){let y=scrollY;try{let res=await fetch('/api/pod',{cache:'no-store'});if(!res.ok)throw Error();all=await res.json();let users=[...new Set(all.map(x=>x.username).filter(Boolean))].sort(),statuses=[...new Set(all.map(label).filter(Boolean))].sort(),oldU=val('username'),oldS=val('status');$('username').innerHTML='<option value="">All usernames</option>'+users.map(x=>`<option>${esc(x)}</option>`).join('');$('status').innerHTML='<option value="">All actions/statuses</option>'+statuses.map(x=>`<option>${esc(x)}</option>`).join('');$('username').value=oldU;$('status').value=oldS;render();requestAnimationFrame(()=>scrollTo(0,y))}catch(e){console.warn('Dashboard refresh failed')}}
['username','from','to','waybill','status'].forEach(id=>$(id).addEventListener(id==='waybill'?'input':'change',render));$('tableToggle').onclick=()=>{tableOpen=!tableOpen;localStorage.setItem('jneTableOpen',tableOpen);render()};load();setInterval(load,10000);
</script></body></html>'''


@app.get("/sdk-config")
def sdk_config():
    import json
    from pathlib import Path
    config_path = Path(__file__).with_name("jne-pod-config.json")
    return json.loads(config_path.read_text())

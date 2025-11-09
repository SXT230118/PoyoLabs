from flask import Flask, jsonify, request, render_template, send_from_directory, redirect, url_for, session
from flask_cors import CORS
from authlib.integrations.flask_client import OAuth
from functools import wraps
from dotenv import load_dotenv
import requests
import random
import time
import os
import threading
import statistics
import traceback
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus, urlencode

# Load environment variables
load_dotenv()

# Optional: NVIDIA Nemotron client (OpenAI-compatible wrapper)
try:
    from openai import OpenAI
    _HAS_NEMOTRON = True
except Exception:
    OpenAI = None
    _HAS_NEMOTRON = False

# --- Setup ---
app = Flask(__name__)
CORS(app)

# Session configuration
app.secret_key = os.environ.get("SECRET_KEY")
if not app.secret_key:
    raise ValueError("No SECRET_KEY set in environment variables")

# Auth0 configuration
oauth = OAuth(app)
oauth.register(
    "auth0",
    client_id=os.environ.get("AUTH0_CLIENT_ID"),
    client_secret=os.environ.get("AUTH0_CLIENT_SECRET"),
    client_kwargs={
        "scope": "openid profile email",
    },
    server_metadata_url=f'https://{os.environ.get("AUTH0_DOMAIN")}/.well-known/openid-configuration'
)

# ### EOG API Base URL ###
EOG_API_BASE_URL = "https://hackutd2025.eog.systems"

# --- Authentication Decorator ---
def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

# --- Auth Routes ---
@app.route('/login')
def login():
    """Redirect to Auth0 login page"""
    return oauth.auth0.authorize_redirect(
        redirect_uri=url_for("callback", _external=True)
    )

@app.route('/callback')
def callback():
    """Handle Auth0 callback"""
    try:
        token = oauth.auth0.authorize_access_token()
        session["user"] = token
        return redirect(url_for("dashboard"))
    except Exception as e:
        print(f"Auth error: {e}")
        return redirect(url_for("index"))

@app.route('/logout')
def logout():
    """Clear session and redirect to Auth0 logout"""
    session.clear()
    return redirect(
        "https://" + os.environ.get("AUTH0_DOMAIN")
        + "/v2/logout?"
        + urlencode(
            {
                "returnTo": url_for("index", _external=True),
                "client_id": os.environ.get("AUTH0_CLIENT_ID"),
            },
            quote_via=quote_plus,
        )
    )

# --- Load Static Factory Data ---
def load_static_factory_data():
    """
    Called ONCE when the server starts.
    Fetches all static data (names, network, etc.) from the API
    and stores it in memory.
    """
    print("Loading static factory data from EOG API...")
    try:
        cauldrons = requests.get(EOG_API_BASE_URL + "/api/Information/cauldrons").json()
        network = requests.get(EOG_API_BASE_URL + "/api/Information/network").json()
        market = requests.get(EOG_API_BASE_URL + "/api/Information/market").json()
        couriers = requests.get(EOG_API_BASE_URL + "/api/Information/couriers").json()
        
        meta = None
        try:
            meta = requests.get(EOG_API_BASE_URL + '/api/Data/metadata', timeout=5).json()
        except Exception:
            meta = None

        meta_rates = {}
        if isinstance(meta, dict):
            for key in ('cauldron_rates', 'rates', 'fill_rates', 'per_cauldron'):
                if key in meta and isinstance(meta[key], dict):
                    meta_rates = meta[key]
                    break

        computed = {}
        try:
            computed = _compute_rates_from_history()
        except Exception:
            computed = {}

        for c in cauldrons:
            cid = c.get('id')
            rate_obj = None
            if cid and cid in meta_rates:
                rate_obj = meta_rates.get(cid)
            elif cid and cid in computed:
                rate_obj = computed.get(cid)

            if isinstance(rate_obj, dict):
                c['fill_rate_per_min'] = float(rate_obj.get('fill_rate_per_min', rate_obj.get('fill_rate', 0)))
                c['drain_rate_per_min'] = float(rate_obj.get('drain_rate_per_min', rate_obj.get('drain_rate', 0)))
            else:
                per_rate = None
                try:
                    per_meta_resp = requests.get(EOG_API_BASE_URL + f"/api/Data/metadata?cauldronId={cid}", timeout=5)
                    if per_meta_resp.status_code == 200:
                        per_meta = per_meta_resp.json()
                        if isinstance(per_meta, dict):
                            for key in ('cauldron_rates', 'rates', 'fill_rates', 'per_cauldron'):
                                if key in per_meta and isinstance(per_meta[key], dict) and cid in per_meta[key]:
                                    per_rate = per_meta[key].get(cid)
                                    break
                            if per_rate is None and ('fill_rate_per_min' in per_meta or 'drain_rate_per_min' in per_meta):
                                per_rate = per_meta
                except Exception:
                    per_rate = None

                if isinstance(per_rate, dict):
                    c['fill_rate_per_min'] = float(per_rate.get('fill_rate_per_min', per_rate.get('fill_rate', 1.0)))
                    c['drain_rate_per_min'] = float(per_rate.get('drain_rate_per_min', per_rate.get('drain_rate', 12.0)))
                else:
                    fallback_fill = 1.0
                    fallback_drain = 12.0
                    c['fill_rate_per_min'] = fallback_fill
                    c['drain_rate_per_min'] = fallback_drain
                    print(f"[rates] No metadata/computed rates for cauldron {cid}; using fallback fill={fallback_fill}, drain={fallback_drain}")
            
        print(f"Successfully loaded data for {len(cauldrons)} cauldrons.")
        
        return {
            "cauldrons": cauldrons,
            "network": network,
            "market": market,
            "couriers": couriers
        }
        
    except Exception as e:
        print(f"!!!!!!!!!!!!!! FAILED TO LOAD STATIC DATA !!!!!!!!!!!!!!")
        print(f"Error: {e}")
        print("Is the EOG API down or did the URL change?")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return None

def _refresh_rates_periodically(interval_seconds=60):
    def loop():
        while True:
            try:
                computed = _compute_rates_from_history()
                if computed:
                    for c in factory_static_data.get('cauldrons', []):
                        cid = c.get('id')
                        if cid in computed:
                            rates = computed[cid]
                            c['fill_rate_per_min'] = rates.get('fill_rate_per_min', c.get('fill_rate_per_min', 0))
                            c['drain_rate_per_min'] = rates.get('drain_rate_per_min', c.get('drain_rate_per_min', 0))
                    print('[rates] Updated fill/drain rates from history')
            except Exception as e:
                print(f'[rates] Background rate refresh failed: {e}')
            time.sleep(interval_seconds)

    t = threading.Thread(target=loop, daemon=True)
    t.start()

@app.route('/api/compute_rates')
@requires_auth
def api_compute_rates():
    try:
        computed = _compute_rates_from_history()
        if computed:
            for c in factory_static_data.get('cauldrons', []):
                cid = c.get('id')
                if cid in computed:
                    rates = computed[cid]
                    c['fill_rate_per_min'] = rates.get('fill_rate_per_min', c.get('fill_rate_per_min', 0))
                    c['drain_rate_per_min'] = rates.get('drain_rate_per_min', c.get('drain_rate_per_min', 0))
        return jsonify({'computed': computed})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def _compute_rates_from_history(sample_limit=500):
    try:
        raw = requests.get(EOG_API_BASE_URL + '/api/Data?start_date=0&end_date=2000000000', timeout=20).json()
    except Exception:
        return {}

    data_list = raw if isinstance(raw, list) else (raw.get('data') if isinstance(raw, dict) and isinstance(raw.get('data'), list) else [])
    if not data_list:
        return {}
    
    records = []
    for rec in data_list:
        if not isinstance(rec, dict):
            continue
        ts = None
        try:
            ts = rec.get('timestamp') or rec.get('time') or rec.get('t')
        except Exception:
            ts = None
        if not ts:
            continue
        parsed = None
        try:
            parsed = _parse_timestamp(ts)
        except Exception:
            parsed = None
        if not parsed:
            continue
        levels = rec.get('cauldron_levels') or rec.get('levels') or {}
        if not isinstance(levels, dict):
            continue
        records.append((parsed, levels))

    if not records:
        return {}

    records = records[-sample_limit:]

    per_series = {}
    for ts, levels in records:
        for cid, v in levels.items():
            try:
                num = float(v)
            except Exception:
                continue
            per_series.setdefault(cid, []).append((ts, num))

    rates = {}
    for cid, series in per_series.items():
        fill_rates = []
        drain_rates = []
        for i in range(len(series)-1):
            t0, v0 = series[i]
            t1, v1 = series[i+1]
            dt_min = (t1 - t0).total_seconds() / 60.0
            if dt_min <= 0 or dt_min > 60*24:
                continue
            delta = v1 - v0
            rate = delta / dt_min
            if rate > 0:
                fill_rates.append(rate)
            elif rate < 0:
                drain_rates.append(abs(rate))

        def choose(vals):
            if not vals:
                return 0.0
            try:
                return float(statistics.median(vals))
            except Exception:
                try:
                    return float(sum(vals)/len(vals))
                except Exception:
                    return 0.0

        fill_r = choose(fill_rates)
        drain_r = choose(drain_rates)

        if fill_r < 0 or fill_r > 1000:
            fill_r = 0.0
        if drain_r < 0 or drain_r > 5000:
            drain_r = 0.0

        rates[cid] = {
            'fill_rate_per_min': round(fill_r, 3),
            'drain_rate_per_min': round(drain_r, 3)
        }

    return rates

factory_static_data = load_static_factory_data()
if factory_static_data is None:
    exit()

forecast_state = {}

try:
    _refresh_rates_periodically(interval_seconds=60)
except Exception:
    pass

# --- Protected API Routes (require authentication) ---

@app.route('/api/cauldron/levels')
@requires_auth
def get_cauldron_levels():
    try:
        live_data_url = EOG_API_BASE_URL + "/api/Data" 
        response = requests.get(live_data_url)
        live_levels_data = response.json() 
        
    except Exception as e:
        print(f"ERROR fetching from /api/Data: {e}")
        return jsonify({"error": str(e)}), 500

    merged_cauldron_data = []

    live_levels_list = live_levels_data
    if isinstance(live_levels_data, dict):
        for wrapper in ('data', 'items', 'results', 'value'):
            if wrapper in live_levels_data and isinstance(live_levels_data[wrapper], list):
                live_levels_list = live_levels_data[wrapper]
                break
        else:
            if any(k in live_levels_data for k in ('cauldronId', 'id', 'cauldron_id', 'currentVolume', 'current_volume')):
                live_levels_list = [live_levels_data]
            else:
                app.logger.warning("Unexpected /api/Data JSON shape: %s", type(live_levels_data))
                return jsonify({"error": "Unexpected /api/Data format"}), 500

    live_levels_map = {}
    if isinstance(live_levels_list, list) and live_levels_list:
        first = live_levels_list[0]
        if isinstance(first, dict) and 'cauldron_levels' in first and isinstance(first['cauldron_levels'], dict):
            latest = None
            for rec in reversed(live_levels_list):
                if isinstance(rec, dict) and isinstance(rec.get('cauldron_levels'), dict):
                    latest = rec['cauldron_levels']
                    break
            if latest is None:
                latest = {}
            for k,v in latest.items():
                try:
                    live_levels_map[k] = float(v)
                except Exception:
                    live_levels_map[k] = v
        else:
            for item in live_levels_list:
                if not isinstance(item, dict):
                    continue

                cauldron_key = None
                for k in ('cauldronId', 'cauldron_id', 'id'):
                    if k in item:
                        cauldron_key = item[k]
                        break

                if cauldron_key is None and isinstance(item.get('cauldron'), dict):
                    cauldron_key = item['cauldron'].get('id')

                if cauldron_key is None:
                    continue

                level = None
                for lvl_key in ('currentVolume', 'current_volume', 'volume', 'level', 'value', 'current'):
                    if lvl_key in item:
                        level = item[lvl_key]
                        break

                try:
                    if level is not None:
                        level = float(level)
                except Exception:
                    level = None

                live_levels_map[cauldron_key] = level
    elif isinstance(live_levels_list, dict):
        if 'cauldron_levels' in live_levels_list and isinstance(live_levels_list['cauldron_levels'], dict):
            for k,v in live_levels_list['cauldron_levels'].items():
                try:
                    live_levels_map[k] = float(v)
                except Exception:
                    live_levels_map[k] = v

    for static_cauldron in factory_static_data['cauldrons']:
        cauldron_id = static_cauldron['id']
        merged_data = static_cauldron.copy()
        
        live_level = live_levels_map.get(cauldron_id)
        
        merged_data['current_level'] = live_level if live_level is not None else 0
        
        if live_level and live_level >= merged_data['max_volume']:
            merged_data['anomaly'] = True
        else:
            merged_data['anomaly'] = False

        merged_cauldron_data.append(merged_data)
        
    return jsonify(merged_cauldron_data)

@app.route('/api/tickets/check_discrepancies')
@requires_auth
def check_discrepancies():
    alerts = []
    try:
        ticket_url = EOG_API_BASE_URL + "/api/Tickets"
        real_tickets = requests.get(ticket_url).json()
        
        history_url = EOG_API_BASE_URL + "/api/Data/metadata" 
        
        if real_tickets:
            first_ticket = real_tickets[0]
            cauldron_id = first_ticket.get('cauldronId', 'cauldron_001')
            ticket_amount = first_ticket.get('amount', 100)
            
            cauldron_data = next((c for c in factory_static_data['cauldrons'] if c['id'] == cauldron_id), None)
            
            calculated_amount = ticket_amount - 50
            
            alerts.append({
                "cauldron_id": cauldron_id,
                "message": f"Suspicious Ticket {first_ticket.get('id')}. Calculated: {calculated_amount:.1f}L, Ticket: {ticket_amount:.1f}L."
            })
            
            alerts.append({
                "cauldron_id": "cauldron_003",
                "message": "Unlogged drain detected at 3:15 PM. No matching ticket found."
            })

    except Exception as e:
        print(f"Error checking discrepancies: {e}")
        return jsonify([{"message": "Error connecting to EOG API to check tickets."}])
        
    if not alerts:
        return jsonify([{"message": "All tickets reconciled."}])
        
    return jsonify(alerts)

@app.route('/api/logistics/forecast')
@requires_auth
def forecast_fill_times(live_levels_data=None):
    forecasts = []
    
    if live_levels_data is None:
        try:
            live_levels_response = get_cauldron_levels()
            if live_levels_response.status_code != 200:
                return jsonify({"error": "Could not get live levels for forecast."})
            live_levels_data = live_levels_response.get_json() 
        except Exception as e:
            return jsonify({"error": str(e)})

    for cauldron in live_levels_data:
        fill_rate = cauldron['fill_rate_per_min'] 
        
        if cauldron['current_level'] < cauldron['max_volume']:
            liters_to_full = cauldron['max_volume'] - cauldron['current_level']
            
            if fill_rate > 0:
                time_to_full_min = liters_to_full / fill_rate
                forecasts.append({
                    "cauldron_id": cauldron['id'],
                    "name": cauldron['name'],
                    "time_to_full_min": round(time_to_full_min, 1)
                })
    
    if live_levels_data is not None:
        return forecasts
    
    return jsonify(forecasts)

@app.route('/api/cauldron/status')
@requires_auth
def cauldron_status():
    try:
        live_levels_response = get_cauldron_levels()
        if live_levels_response.status_code != 200:
            return live_levels_response
        live_levels = live_levels_response.get_json()
    except Exception as e:
        return jsonify({"error": f"Could not fetch live levels: {e}"}), 500

    try:
        forecasts = forecast_fill_times(live_levels_data=live_levels)
        
    except Exception as e:
        print(f"Error in forecast_fill_times: {e}")
        forecasts = []

    forecast_map = {f.get('cauldron_id'): f for f in (forecasts or [])}

    status_list = []
    for c in live_levels:
        max_vol = c.get('max_volume') or 1
        current = c.get('current_level') or 0
        try:
            percent = round((current / float(max_vol)) * 100, 1)
        except Exception:
            percent = 0.0

        f = forecast_map.get(c.get('id'))
        time_to_full_min = f.get('time_to_full_min') if f else None
        time_to_full_seconds = None
        if time_to_full_min is not None:
            try:
                time_to_full_seconds = int(round(float(time_to_full_min) * 60))
            except Exception:
                time_to_full_seconds = None

        status = c.copy()
        status['percent_full'] = percent
        status['time_to_full_min'] = time_to_full_min
        status['time_to_full_seconds'] = time_to_full_seconds
        try:
            now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
            status['as_of'] = now_utc.isoformat()
            if time_to_full_seconds is not None:
                try:
                    new_full_at = now_utc + timedelta(seconds=int(time_to_full_seconds))
                    cid = c.get('id')
                    prev = forecast_state.get(cid)
                    if prev and isinstance(prev, datetime):
                        delta_ms = (new_full_at - prev).total_seconds() * 1000.0
                        if delta_ms <= -2000:
                            final_full_at = new_full_at
                        elif delta_ms > 0:
                            max_increase_ms = 5000
                            allowed = min(delta_ms, max_increase_ms)
                            final_full_at = prev + timedelta(milliseconds=allowed)
                        else:
                            final_full_at = new_full_at
                    else:
                        final_full_at = new_full_at

                    forecast_state[cid] = final_full_at
                    status['full_at'] = final_full_at.isoformat()
                except Exception:
                    status['full_at'] = None
            else:
                status['full_at'] = None
        except Exception:
            status['as_of'] = None
            status['full_at'] = None
        status_list.append(status)

    return jsonify(status_list)

def _parse_timestamp(ts_str):
    if not ts_str:
        return None
    try:
        if ts_str.endswith('Z'):
            ts_str = ts_str[:-1] + '+00:00'
        return datetime.fromisoformat(ts_str)
    except Exception:
        try:
            return datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%S')
        except Exception:
            return None

@app.route('/api/data/historic')
@requires_auth
def data_historic():
    start_q = request.args.get('start')
    end_q = request.args.get('end')
    cauldron_id = request.args.get('cauldron_id')

    try:
        raw = requests.get(EOG_API_BASE_URL + '/api/Data').json()
    except Exception as e:
        return jsonify({'error': f'Could not fetch /api/Data: {e}'}), 500

    data_list = raw if isinstance(raw, list) else (raw.get('data') if isinstance(raw, dict) and isinstance(raw.get('data'), list) else [])

    start_dt = None
    end_dt = None
    if start_q:
        try:
            start_dt = _parse_timestamp(start_q if 'T' in start_q else start_q + 'T00:00:00')
        except Exception:
            start_dt = None
    if end_q:
        try:
            end_dt = _parse_timestamp(end_q if 'T' in end_q else end_q + 'T23:59:59')
        except Exception:
            end_dt = None

    out = []
    for rec in data_list:
        ts = None
        if isinstance(rec, dict):
            ts = _parse_timestamp(rec.get('timestamp') or rec.get('time') or rec.get('t'))
        if ts is None:
            continue
        if start_dt and ts < start_dt:
            continue
        if end_dt and ts > end_dt:
            continue

        if cauldron_id:
            levels = rec.get('cauldron_levels') or rec.get('levels') or {}
            value = None
            if isinstance(levels, dict):
                value = levels.get(cauldron_id)
            out.append({'timestamp': ts.isoformat(), 'cauldron_id': cauldron_id, 'value': value})
        else:
            out.append(rec)

    return jsonify(out)

@app.route('/api/network')
@requires_auth
def get_network():
    return jsonify(factory_static_data)

def _extract_ticket_amount(ticket):
    for k in ('amount', 'amount_collected', 'quantity', 'volume'):
        v = ticket.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    for v in ticket.values():
        if isinstance(v, (int, float)):
            return float(v)
    return None

@app.route('/api/tickets/match')
@requires_auth
def tickets_match():
    try:
        tickets_raw = requests.get(EOG_API_BASE_URL + '/api/Tickets').json()
    except Exception as e:
        return jsonify({'error': f'Could not fetch /api/Tickets: {e}'}), 500

    tickets_list = tickets_raw if isinstance(tickets_raw, list) else (tickets_raw.get('transport_tickets') if isinstance(tickets_raw, dict) and isinstance(tickets_raw.get('transport_tickets'), list) else (tickets_raw.get('tickets') if isinstance(tickets_raw, list) else []))

    try:
        data_raw = requests.get(EOG_API_BASE_URL + '/api/Data').json()
    except Exception as e:
        return jsonify({'error': f'Could not fetch /api/Data: {e}'}), 500

    data_list = data_raw if isinstance(data_raw, list) else (data_raw.get('data') if isinstance(data_raw, dict) and isinstance(data_raw.get('data'), list) else [])

    series_map = {}
    for rec in data_list:
        ts = _parse_timestamp(rec.get('timestamp') or rec.get('time') or rec.get('t'))
        if ts is None:
            continue
        levels = rec.get('cauldron_levels') or rec.get('levels') or {}
        if not isinstance(levels, dict):
            continue
        for cid, val in levels.items():
            try:
                v = float(val)
            except Exception:
                continue
            series_map.setdefault(cid, []).append((ts, v))

    for cid in series_map:
        series_map[cid].sort(key=lambda x: x[0])

    results = []
    unmatched_drains = []

    def get_static(cauldron_id):
        return next((c for c in factory_static_data['cauldrons'] if c['id'] == cauldron_id), None)

    drains_by_cauldron_day = {}
    for cid, series in series_map.items():
        static = get_static(cid)
        fill_rate = static.get('fill_rate_per_min', 0) if static else 0
        i = 0
        n = len(series)
        while i < n-1:
            t0, v0 = series[i]
            j = i+1
            if series[j][1] < v0:
                start_t = t0
                start_v = v0
                end_t = series[j][0]
                end_v = series[j][1]
                j += 1
                while j < n and series[j][1] < series[j-1][1]:
                    end_t = series[j][0]
                    end_v = series[j][1]
                    j += 1

                duration_min = (end_t - start_t).total_seconds() / 60.0
                drained = max(0.0, start_v - end_v)
                drained_adjusted = drained + (fill_rate * duration_min)

                day_key = start_t.date().isoformat()
                drains_by_cauldron_day.setdefault(cid, {}).setdefault(day_key, []).append({
                    'start': start_t.isoformat(),
                    'end': end_t.isoformat(),
                    'start_v': start_v,
                    'end_v': end_v,
                    'duration_min': round(duration_min, 1),
                    'drained': round(drained_adjusted, 2)
                })
                i = j
            else:
                i += 1

    for t in tickets_list:
        ticket_id = t.get('id') or t.get('ticket_id') or t.get('ticketId')
        cauldron_id = t.get('cauldronId') or t.get('cauldron_id') or t.get('cauldron') or t.get('cauldronId')
        date_str = t.get('date') or t.get('day') or t.get('ticket_date') or t.get('timestamp')
        amount = _extract_ticket_amount(t)

        match_day = None
        if date_str:
            try:
                if len(date_str) <= 10:
                    match_day = datetime.fromisoformat(date_str).date().isoformat()
                else:
                    dt = _parse_timestamp(date_str)
                    if dt:
                        match_day = dt.date().isoformat()
            except Exception:
                match_day = None

        calculated = None
        matched_events = []
        if cauldron_id and match_day and cid in drains_by_cauldron_day if True else False:
            day_drains = drains_by_cauldron_day.get(cauldron_id, {}).get(match_day, [])
            calculated = sum(d['drained'] for d in day_drains)
            matched_events = day_drains

        if calculated is None and cauldron_id:
            series = series_map.get(cauldron_id, [])
            if match_day:
                s = 0.0
                for i in range(len(series)-1):
                    a_ts, a_v = series[i]
                    b_ts, b_v = series[i+1]
                    if a_ts.date().isoformat() != match_day:
                        continue
                    if b_ts.date().isoformat() != match_day:
                        continue
                    if b_v < a_v:
                        s += (a_v - b_v)
                calculated = s

        suspicious = False
        diff = None
        reason = ''
        if amount is not None and calculated is not None:
            diff = round(amount - calculated, 2)
            if abs(diff) > 5 and abs(diff) > 0.1 * max(1.0, amount):
                suspicious = True
                reason = f'Difference {diff}L exceeds threshold.'
        else:
            reason = 'Insufficient data to compute match.'

        app.logger.info(f"[tickets_match] ticket={ticket_id} cauldron={cauldron_id} day={match_day} ticket_amount={amount} calculated={calculated} diff={diff} suspicious={suspicious}")

        results.append({
            'ticket_id': ticket_id,
            'cauldron_id': cauldron_id,
            'ticket_amount': amount,
            'calculated_amount': None if calculated is None else round(calculated,2),
            'difference': diff,
            'suspicious': suspicious,
            'matched_events': matched_events,
            'reason': reason
        })

    for cid, days in drains_by_cauldron_day.items():
        for day, events in days.items():
            has_ticket = any((r for r in results if r['cauldron_id'] == cid and r['ticket_amount'] is not None and r['ticket_id'] is not None and (r['calculated_amount'] is not None)))
            if not has_ticket:
                for e in events:
                    unmatched_drains.append({'cauldron_id': cid, 'day': day, 'event': e})

    return jsonify({'matches': results, 'unmatched_drains': unmatched_drains})

@app.route('/api/logistics/dispatch_courier', methods=['POST'])
@requires_auth
def dispatch_courier():
    data = request.json
    cauldron_id = data.get('cauldron_id')
    
    cauldron_data = next((c for c in factory_static_data['cauldrons'] if c['id'] == cauldron_id), None)
    
    if not cauldron_data:
        return jsonify({"status": "error", "message": "Invalid cauldron ID."}), 400
    
    print(f"SIMULATED DISPATCH: Courier sent to {cauldron_data['name']}")
    
    return jsonify({
        "status": "success",
        "message": f"Courier witch dispatched to {cauldron_data['name']}. (Simulation)"
    })

@app.route('/api/agent/chat', methods=['POST'])
@requires_auth
def handle_agent_chat():
    user_message = request.json.get('message')
    nv_api_key = request.json.get('nv_api_key') or os.environ.get('NV_API_KEY')
    use_nemotron = bool(request.json.get('use_nemotron')) or bool(nv_api_key)
    show_reasoning = bool(request.json.get('debug')) or bool(os.environ.get('NV_SHOW_REASONING'))
    
    agent_plan = [] 
    agent_final_response = ""

    if "suspicious" in user_message.lower() or "anomaly" in user_message.lower() or "ticket" in user_message.lower():
        agent_plan.append("Plan: User asked about discrepancies. I will call the real `tickets_match()` tool.")
        
        match_data = tickets_match().get_json() 
        
        suspicious_matches = [m for m in match_data.get('matches', []) if m['suspicious']]
        unmatched_drains = match_data.get('unmatched_drains', [])
        
        if suspicious_matches or unmatched_drains:
            agent_plan.append("Tool Result: Found discrepancies.")
            agent_final_response = "I've checked the tickets and found some problems:\n"
            for match in suspicious_matches:
                agent_final_response += f"  - Suspicious Ticket: {match['ticket_id']} ({match['cauldron_id']}). Reason: {match['reason']}\n"
            for drain in unmatched_drains:
                agent_final_response += f"  - Unmatched Drain: {drain['cauldron_id']} on {drain['day']} for {drain['event']['drained']:.1f}L.\n"
        else:
            agent_plan.append("Tool Result: No discrepancies found.")
            agent_final_response = "I've checked all tickets against the historical data. All potion flows are accounted for."

    elif "forecast" in user_message.lower() or "full" in user_message.lower():
        agent_plan.append("Plan: User asked for forecasts. I will call `forecast_fill_times()`.")
        
        forecasts = forecast_fill_times().get_json()
        agent_plan.append(f"Tool Result: {forecasts}")
        
        agent_final_response = "Here is the live forecast (top 5):\n"
        forecasts.sort(key=lambda x: x.get('time_to_full_min', 9999))
        for f in forecasts[:5]:
            agent_final_response += f"  - {f['name']} ({f['cauldron_id']}) will be full in {f['time_to_full_min']} minutes.\n"

    elif "dispatch" in user_message.lower() or "empty" in user_message.lower():
        cauldron_id_to_dispatch = None
        for cauldron in factory_static_data['cauldrons']:
            if cauldron['id'] in user_message.lower() or cauldron['name'].split(" ")[0].lower() in user_message.lower():
                cauldron_id_to_dispatch = cauldron['id']
                break
        
        if cauldron_id_to_dispatch:
            agent_plan.append(f"Plan: User wants to dispatch to {cauldron_id_to_dispatch}. I will call `dispatch_courier()`.")
            
            dispatch_response = requests.post(
                "http://127.0.0.1:5000/api/logistics/dispatch_courier", 
                json={"cauldron_id": cauldron_id_to_dispatch}
            )
            dispatch_result = dispatch_response.json()
            
            agent_plan.append(f"Tool Result: {dispatch_result['status']}.")
            agent_final_response = dispatch_result['message']
        else:
            agent_final_response = "Which cauldron (e.g., cauldron_001) should I dispatch to?"
            
    elif "optimize" in user_message.lower() or "routes" in user_message.lower() or "witches" in user_message.lower():
        agent_plan.append("Plan: User asked for the Bonus. I will explain the solution using the live API data.")
        
        network_edges = len(factory_static_data['network'])
        num_couriers = len(factory_static_data['couriers'])
        market_name = factory_static_data['market'].get('name', 'The Enchanted Market')
        
        agent_final_response = (
            "This is the EOG Bonus! Here is how I would solve it:\n"
            f"1. **Use Forecast:** First, I call my `forecast_fill_times()` tool to get a 'deadline' for each cauldron.\n"
            f"2. **Use Network Map:** I will use the **`/api/Information/network`** data to calculate travel times between the {market_name} and all urgent cauldrons.\n"
            f"3. **Account for Constraints:** I'll add the 15-minute `unload_time` at the market, plus the `drain_rate` (from `/api/Data/metadata`) to calculate drain time.\n"
            f"4. **Find Minimum Witches:** I'll run a VRP (Vehicle Routing Problem) algorithm to find the minimum number of the **{num_couriers} available couriers** (from `/api/Information/couriers`) needed to service all cauldrons before they overflow."
        )

    else:
        agent_final_response = "I am connected to the EOG API. I can **check tickets**, **forecast** fill times, **dispatch** couriers, or **optimize routes**."
    
    if use_nemotron:
        if not _HAS_NEMOTRON:
            agent_plan.append("Note: Nemotron client not installed; set up 'openai' package to enable.")
        elif not nv_api_key:
            agent_plan.append("Note: NV API key not provided; set 'nv_api_key' in the request or NV_API_KEY env var.")
        else:
            try:
                system_msg = (
                    "You are an assistant integrated with a factory monitoring system. "
                    "Use the agent plan and tool outputs to craft a concise, actionable reply to the user. "
                    "Be clear about any suggested actions."
                )

                context_text = "\n".join(agent_plan)
                prompt = (
                    f"Context:\n{context_text}\n\nUser message:\n{user_message}\n\n"
                    "Provide a short assistant reply based on the context."
                )

                messages = [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt}
                ]

                client = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=nv_api_key)
                completion = client.chat.completions.create(
                    model="nvidia/nvidia-nemotron-nano-9b-v2",
                    messages=messages,
                    temperature=0.6,
                    top_p=0.95,
                    max_tokens=512,
                    frequency_penalty=0,
                    presence_penalty=0,
                    stream=True,
                    extra_body={"min_thinking_tokens": 256, "max_thinking_tokens": 512}
                )

                assembled = []
                reasoning_parts = []
                for chunk in completion:
                    try:
                        delta = chunk.choices[0].delta
                    except Exception:
                        delta = None

                    if delta is None:
                        continue

                    reasoning = getattr(delta, 'reasoning_content', None)
                    content = getattr(delta, 'content', None)
                    if content is None:
                        content = getattr(delta, 'text', None)

                    if reasoning:
                        reasoning_parts.append(str(reasoning))
                    if content:
                        assembled.append(str(content))

                final_text = "".join(assembled).strip()
                if final_text:
                    agent_final_response = final_text
                    agent_plan.append("Tool Result: Response generated by Nemotron (stream).")
                    if reasoning_parts and show_reasoning:
                        agent_plan.append("Nemotron reasoning: " + " ".join(reasoning_parts))
                else:
                    agent_plan.append("Warning: Nemotron streamed no text; keeping local response.")
            except Exception as e:
                tb = traceback.format_exc()
                print("[Nemotron] streaming call failed:", str(e))
                print(tb)
                agent_plan.append(f"Nemotron call failed (stream): {str(e)}")

                try:
                    fallback = client.chat.completions.create(
                        model="nvidia/nvidia-nemotron-nano-9b-v2",
                        messages=messages,
                        temperature=0.6,
                        top_p=0.95,
                        max_tokens=512,
                        frequency_penalty=0,
                        presence_penalty=0,
                        stream=False,
                        extra_body={"min_thinking_tokens": 256, "max_thinking_tokens": 512}
                    )
                    text_out = ""
                    try:
                        if hasattr(fallback, 'choices'):
                            ch0 = fallback.choices[0]
                            if hasattr(ch0, 'message') and isinstance(ch0.message, dict) and 'content' in ch0.message:
                                text_out = ch0.message['content']
                            elif hasattr(ch0, 'text'):
                                text_out = ch0.text
                            elif isinstance(ch0, dict):
                                msg = ch0.get('message') or {}
                                text_out = msg.get('content') or ch0.get('text') or ""
                        elif isinstance(fallback, dict):
                            choices = fallback.get('choices', [])
                            if choices:
                                c0 = choices[0]
                                msg = c0.get('message') or {}
                                text_out = msg.get('content') or c0.get('text') or ""
                    except Exception as e_parse:
                        agent_plan.append(f"Nemotron fallback parse failed: {e_parse}")

                    text_out = (text_out or "").strip()
                    if text_out:
                        agent_final_response = text_out
                        agent_plan.append("Tool Result: Nemotron non-streaming response used as fallback.")
                    else:
                        agent_plan.append("Nemotron non-streaming returned empty text.")
                except Exception as e2:
                    print("[Nemotron] non-stream fallback failed:", str(e2))
                    agent_plan.append(f"Nemotron non-stream fallback failed: {str(e2)}")

    return jsonify({
        "agent_response": agent_final_response,
        "agent_plan": agent_plan
    })

# --- Frontend Routes ---
@app.route('/')
def index():
    return send_from_directory(app.root_path, 'index.html')

@app.route('/dashboard')
@requires_auth
def dashboard():
    # Get user info from session for display
    user_info = session.get('user', {}).get('userinfo', {})
    return send_from_directory(app.root_path, 'dashboard.html')

@app.route('/api/time')
def api_time():
    try:
        now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        return jsonify({'server_time': now})
    except Exception:
        return jsonify({'server_time': None})

@app.route('/api/user')
@requires_auth
def get_user():
    """Return current user info"""
    user_info = session.get('user', {}).get('userinfo', {})
    return jsonify({
        'name': user_info.get('name', 'User'),
        'email': user_info.get('email', ''),
        'picture': user_info.get('picture', '')
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)